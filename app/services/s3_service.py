"""S3操作サービスクラス

S3バケットへのアクセスを抽象化し、以下の機能を提供する:
- オブジェクト一覧取得
- オブジェクト内容取得
- 事前署名URL生成
- 再帰的オブジェクト一覧取得（バッチダウンロード用）
- バッチオブジェクト取得（バッチダウンロード用）

注意: このクラスはStorageServiceのラッパーとして動作し、
      STORAGE_MODE環境変数に応じてS3またはローカルFSを使用する。
"""

import os
from typing import Optional, List, Generator, Tuple
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import logging

from services.storage_service import get_storage, StorageService

logger = logging.getLogger(__name__)


class S3Service:
    """
    S3操作を行うサービスクラス

    StorageServiceをバックエンドとして使用し、
    環境変数STORAGE_MODEに応じてS3またはローカルFSを透過的に切り替える。
    """

    def __init__(self):
        """StorageServiceを初期化"""
        self._storage = get_storage()
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'labcode-dev-artifacts')
        logger.info(f"S3Service initialized: mode={self._storage.mode}")

    def list_objects(
        self,
        prefix: str,
        delimiter: str = '/'
    ) -> dict:
        """
        オブジェクト一覧を取得する

        Args:
            prefix: S3プレフィックス
            delimiter: 階層区切り文字（デフォルト: '/'）

        Returns:
            dict: {'contents': [...], 'common_prefixes': [...]}

        Raises:
            ClientError: S3アクセスエラー（S3モードのみ）
        """
        return self._storage.list_objects_with_dirs(prefix, delimiter)

    def get_object(self, key: str) -> dict:
        """
        オブジェクトを取得する

        Args:
            key: S3キー

        Returns:
            dict: {'body': bytes, 'content_length': int, 'last_modified': datetime}

        Raises:
            ClientError: S3アクセスエラー（NoSuchKey含む）
        """
        content = self._storage.load(key)
        if content is None:
            # ClientErrorを模倣してNoSuchKeyエラーを発生
            from botocore.exceptions import ClientError
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Not Found'}},
                'GetObject'
            )

        metadata = self._storage.get_metadata(key)
        return {
            'body': content,
            'content_length': metadata['content_length'] if metadata else len(content),
            'last_modified': metadata['last_modified'] if metadata else datetime.now()
        }

    def head_object(self, key: str) -> dict:
        """
        オブジェクトのメタデータを取得する（存在確認用）

        Args:
            key: S3キー

        Returns:
            dict: {'content_length': int, 'last_modified': datetime}

        Raises:
            ClientError: S3アクセスエラー（NoSuchKey含む）
        """
        metadata = self._storage.get_metadata(key)
        if metadata is None:
            from botocore.exceptions import ClientError
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Not Found'}},
                'HeadObject'
            )

        return {
            'content_length': metadata['content_length'],
            'last_modified': metadata['last_modified']
        }

    def generate_presigned_url(
        self,
        key: str,
        expires_in: int = 3600
    ) -> str:
        """
        事前署名URLを生成する

        Args:
            key: S3キー
            expires_in: 有効期限（秒）、デフォルト3600秒（1時間）

        Returns:
            str: 事前署名URL（ローカルモードではNone）
        """
        url = self._storage.generate_presigned_url(key, expires_in)
        if url is None and self._storage.mode == 'local':
            # ローカルモードでは直接ダウンロードAPIを使用する必要がある
            logger.warning(f"Presigned URL not available in local mode for: {key}")
            # APIエンドポイントを返す（フロントエンドで対応が必要）
            return f"/api/storage/download-direct?file_path={key}"
        return url

    def list_objects_recursive(self, prefix: str) -> List[dict]:
        """
        指定プレフィックス配下の全オブジェクトを再帰的に取得する

        Args:
            prefix: S3プレフィックス（例: runs/1/）

        Returns:
            List[dict]: オブジェクト情報リスト
                各要素: {'Key': str, 'Size': int, 'LastModified': datetime}

        Raises:
            ClientError: S3アクセスエラー
        """
        return self._storage.list_objects(prefix)

    def get_object_stream(self, key: str) -> Generator[bytes, None, None]:
        """
        オブジェクトをストリーミングで取得する

        Args:
            key: S3キー

        Yields:
            bytes: ファイルチャンク（64KB単位）

        Raises:
            ClientError: S3アクセスエラー
        """
        return self._storage.load_stream(key, chunk_size=64 * 1024)

    def get_objects_batch(
        self,
        keys: List[str]
    ) -> Generator[Tuple[str, bytes], None, None]:
        """
        複数オブジェクトをバッチ取得する（ジェネレータ）

        Args:
            keys: S3キーリスト

        Yields:
            Tuple[str, bytes]: (キー, コンテンツ)

        Note:
            エラーが発生したキーはスキップし、ログに記録する
        """
        for key in keys:
            try:
                content = self._storage.load(key)
                if content is not None:
                    yield (key, content)
                else:
                    logger.warning(f"Failed to get object {key}: Not found")
            except Exception as e:
                logger.warning(f"Failed to get object {key}: {e}")
                continue

    def calculate_total_size(self, prefix: str) -> int:
        """
        指定プレフィックス配下の全オブジェクトの合計サイズを計算する

        Args:
            prefix: S3プレフィックス

        Returns:
            int: 合計サイズ（バイト）
        """
        return self._storage.calculate_total_size(prefix)


def get_content_type(extension: str) -> str:
    """
    拡張子からコンテンツタイプを判定する

    Args:
        extension: ファイル拡張子（小文字）

    Returns:
        str: 'text', 'json', 'yaml', または 'binary'
    """
    text_types = {'txt', 'log', 'md', 'rst', 'csv'}
    json_types = {'json'}
    yaml_types = {'yaml', 'yml'}

    if extension in text_types:
        return 'text'
    elif extension in json_types:
        return 'json'
    elif extension in yaml_types:
        return 'yaml'
    else:
        return 'binary'
