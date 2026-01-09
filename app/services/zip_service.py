"""ZIPストリーミング生成サービス

複数ランのファイルをZIP形式でストリーミング生成する。
メモリ効率を考慮し、ファイルを逐次追加しながらZIPストリームを生成する。
"""

import json
import zipstream
from datetime import datetime
from typing import List, Generator, Dict, Any, Optional
import logging

from services.s3_service import S3Service

logger = logging.getLogger(__name__)

# サイズ制限（バイト）
MAX_ZIP_SIZE = 500 * 1024 * 1024  # 500MB
MAX_SINGLE_FILE_SIZE = 100 * 1024 * 1024  # 100MB
MAX_RUN_COUNT = 100


class ZipServiceError(Exception):
    """ZIPサービスエラーの基底クラス"""
    pass


class SizeLimitExceededError(ZipServiceError):
    """サイズ制限超過エラー"""
    pass


class RunNotFoundError(ZipServiceError):
    """ラン未検出エラー"""
    pass


class ZipStreamService:
    """
    ストリーミングZIP生成サービス

    メモリ効率を考慮し、ファイルを逐次追加しながら
    ZIPストリームを生成する。
    """

    def __init__(self, s3_service: Optional[S3Service] = None):
        """
        初期化

        Args:
            s3_service: S3サービスインスタンス（テスト用にDI可能）
        """
        self.s3_service = s3_service or S3Service()

    def create_zip_stream(
        self,
        runs: List[Dict[str, Any]],
        include_manifest: bool = True
    ) -> Generator[bytes, None, None]:
        """
        ZIPストリームを生成する

        Args:
            runs: ランオブジェクトリスト
                各要素: {'id': int, 'storage_address': str, 'file_name': str, 'status': str}
            include_manifest: manifestファイルを含めるか

        Yields:
            bytes: ZIPストリームチャンク

        Raises:
            SizeLimitExceededError: サイズ制限超過時
            RunNotFoundError: ランが見つからない時
        """
        if len(runs) > MAX_RUN_COUNT:
            raise SizeLimitExceededError(
                f"ラン数が上限（{MAX_RUN_COUNT}件）を超えています"
            )

        # ZIPストリームを作成
        z = zipstream.ZipFile(mode='w', compression=zipstream.ZIP_DEFLATED)

        # manifest用のデータ収集
        manifest_data = {
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'runs': [],
            'errors': [],
            'total_files': 0,
            'total_size': 0
        }

        # 各ランを処理
        for run in runs:
            run_id = run.get('id')
            storage_address = run.get('storage_address', '')

            if not storage_address:
                logger.warning(f"Run {run_id}: storage_address is empty, skipping")
                manifest_data['errors'].append({
                    'run_id': run_id,
                    'error': 'storage_address is empty',
                    'skipped': True
                })
                continue

            try:
                # S3からファイル一覧を取得
                prefix = storage_address.rstrip('/') + '/'
                objects = self.s3_service.list_objects_recursive(prefix)

                if not objects:
                    logger.warning(f"Run {run_id}: No files found at {prefix}")
                    manifest_data['errors'].append({
                        'run_id': run_id,
                        'error': 'No files found',
                        'skipped': True
                    })
                    continue

                # サイズチェック
                total_run_size = sum(obj['Size'] for obj in objects)
                if manifest_data['total_size'] + total_run_size > MAX_ZIP_SIZE:
                    raise SizeLimitExceededError(
                        f"合計サイズが上限（{MAX_ZIP_SIZE // (1024*1024)}MB）を超えます"
                    )

                run_file_count = 0

                # 各ファイルをZIPに追加
                for obj in objects:
                    key = obj['Key']
                    size = obj['Size']

                    # 大きすぎるファイルはスキップ
                    if size > MAX_SINGLE_FILE_SIZE:
                        logger.warning(
                            f"File {key} exceeds size limit ({size} bytes), skipping"
                        )
                        continue

                    # ZIP内のパスを決定
                    # storage_address以降の相対パスを使用
                    relative_path = key[len(prefix):]
                    zip_path = f"run_{run_id}/{relative_path}"

                    # ファイルコンテンツのジェネレータを作成
                    z.write_iter(
                        zip_path,
                        self._file_content_generator(key)
                    )

                    run_file_count += 1
                    manifest_data['total_size'] += size

                # manifest用のラン情報を記録
                manifest_data['runs'].append({
                    'run_id': run_id,
                    'file_name': run.get('file_name', ''),
                    'status': run.get('status', ''),
                    'file_count': run_file_count,
                    'total_size': total_run_size
                })
                manifest_data['total_files'] += run_file_count

            except SizeLimitExceededError:
                raise
            except Exception as e:
                logger.error(f"Run {run_id}: Error processing - {e}")
                manifest_data['errors'].append({
                    'run_id': run_id,
                    'error': str(e),
                    'skipped': True
                })

        # manifestファイルを追加
        if include_manifest:
            manifest_json = json.dumps(manifest_data, indent=2, ensure_ascii=False)
            z.writestr('manifest.json', manifest_json.encode('utf-8'))

        # ZIPストリームを出力
        for chunk in z:
            yield chunk

    def _file_content_generator(self, key: str) -> Generator[bytes, None, None]:
        """
        S3からファイルコンテンツを取得するジェネレータ

        Args:
            key: S3キー

        Yields:
            bytes: ファイルチャンク
        """
        try:
            for chunk in self.s3_service.get_object_stream(key):
                yield chunk
        except Exception as e:
            logger.error(f"Error reading file {key}: {e}")
            # 空のファイルとして処理
            yield b''

    def estimate_zip_size(self, run_ids: List[int], runs_data: List[Dict[str, Any]]) -> int:
        """
        ZIPファイルの推定サイズを計算する

        Args:
            run_ids: ランIDリスト
            runs_data: ランデータリスト

        Returns:
            int: 推定サイズ（バイト）
        """
        total_size = 0
        run_map = {run['id']: run for run in runs_data}

        for run_id in run_ids:
            run = run_map.get(run_id)
            if not run or not run.get('storage_address'):
                continue

            prefix = run['storage_address'].rstrip('/') + '/'
            try:
                total_size += self.s3_service.calculate_total_size(prefix)
            except Exception as e:
                logger.warning(f"Could not calculate size for run {run_id}: {e}")

        return total_size

    def generate_filename(self) -> str:
        """
        ZIPファイル名を生成する

        Returns:
            str: ファイル名（例: labcode_runs_20251221_120000.zip）
        """
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        return f"labcode_runs_{timestamp}.zip"
