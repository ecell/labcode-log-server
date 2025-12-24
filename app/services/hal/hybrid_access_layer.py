"""Hybrid Access Layer (HAL)

S3/ローカルファイルとDBデータを統一的に扱うアクセス抽象化レイヤー。
Run.storage_modeとデータ種別に基づいて適切なデータソースを選択。

レジストリパターンを使用して、バックエンドを動的に取得。
"""

import os
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

from .models import StorageMode, ContentType, DataSource, ContentItem, StorageInfo
from .db_backend import DBDataBackend

logger = logging.getLogger(__name__)


class HybridAccessLayer:
    """
    ハイブリッドアクセスレイヤー

    S3/ローカルファイルとDBデータを統一的に扱う。
    Run.storage_modeとデータ種別に基づいて適切なデータソースを選択。
    storage_mode=nullの場合は、データの存在を確認してモードを推論する。
    """

    def __init__(self, db_session: Session):
        self._db = db_session
        self._db_backend = DBDataBackend(db_session)

        # ファイルバックエンドは遅延初期化（辞書で動的管理）
        self._file_backends: Dict[str, Any] = {}
        # 推論結果のキャッシュ（run_id -> StorageMode）
        self._inferred_mode_cache: Dict[int, StorageMode] = {}

    def _get_backend(self, mode_str: str):
        """
        モード名に対応するバックエンドを取得（遅延初期化）

        レジストリパターンを使用して動的にバックエンドを取得。
        """
        if mode_str not in self._file_backends:
            from services.storage_service import BackendRegistry, StorageConfig
            config = StorageConfig.from_env()

            if BackendRegistry.is_registered(mode_str):
                backend_class = BackendRegistry.get(mode_str)
                # モードに応じた設定を取得
                if mode_str == 's3':
                    self._file_backends[mode_str] = backend_class(config.s3)
                elif mode_str == 'local':
                    self._file_backends[mode_str] = backend_class(config.local)
                else:
                    # 新しいバックエンド用（設定はNoneで初期化）
                    self._file_backends[mode_str] = backend_class(None)
            else:
                # 未登録の場合はS3をフォールバック
                backend_class = BackendRegistry.get('s3')
                self._file_backends[mode_str] = backend_class(config.s3)

        return self._file_backends[mode_str]

    def _get_s3_backend(self):
        """S3バックエンドを取得（後方互換性）"""
        return self._get_backend('s3')

    def _get_local_backend(self):
        """ローカルバックエンドを取得（後方互換性）"""
        return self._get_backend('local')

    def _get_run(self, run_id: int):
        """Runエンティティを取得"""
        from define_db.models import Run
        run = self._db.query(Run).filter(
            Run.id == run_id,
            Run.deleted_at.is_(None)
        ).first()
        if not run:
            raise ValueError(f"Run {run_id} not found")
        return run

    def _get_storage_mode(self, run) -> StorageMode:
        """Runのストレージモードを取得"""
        return StorageMode.from_string(run.storage_mode)

    def _infer_storage_mode(self, run) -> StorageMode:
        """
        storage_mode=nullのRunに対してストレージモードを推論

        推論アルゴリズム:
        1. storage_modeが明示的に設定されている場合はそのまま返す
        2. storage_mode=nullの場合:
           a. DBにオペレーションログ（Operation.log）が存在すれば LOCAL
           b. S3にファイルが存在すれば S3
           c. どちらにもなければ UNKNOWN

        Args:
            run: Runエンティティ

        Returns:
            推論されたStorageMode
        """
        # 明示的にモードが設定されている場合
        if run.storage_mode:
            return StorageMode.from_string(run.storage_mode)

        # キャッシュをチェック
        if run.id in self._inferred_mode_cache:
            return self._inferred_mode_cache[run.id]

        # 推論開始
        inferred = self._do_infer_storage_mode(run)
        self._inferred_mode_cache[run.id] = inferred

        if inferred != StorageMode.UNKNOWN:
            logger.info(f"Inferred storage_mode for Run {run.id}: {inferred.value}")

        return inferred

    def _do_infer_storage_mode(self, run, persist: bool = True) -> StorageMode:
        """実際の推論処理

        推論優先順位（重要）:
        1. S3にファイルがあれば S3（レガシーデータはS3保存が主）
        2. S3にファイルがなく、DBにログがあれば LOCAL（ローカルモード専用）
        3. どちらにもなければ UNKNOWN

        この優先順位は、レガシーデータ（storage_mode=null）が
        S3に保存されている可能性が高いことに基づいています。

        Args:
            run: Runエンティティ
            persist: TrueならDBに永続化する（デフォルト: True）
        """
        from define_db.models import Operation, Process

        # Step 1: S3にファイルがあるか確認（優先）
        try:
            s3_backend = self._get_s3_backend()
            storage_address = run.storage_address or f"runs/{run.id}/"
            result = s3_backend.list_objects_with_dirs(storage_address)
            has_s3_files = bool(result.get('contents', []))
            if has_s3_files:
                inferred = StorageMode.S3
                if persist:
                    self._persist_inferred_mode(run, inferred)
                return inferred
        except Exception as e:
            logger.debug(f"S3 check failed for Run {run.id}: {e}")

        # Step 2: DBにオペレーションログがあるか確認
        has_db_logs = self._db.query(Operation).join(Process).filter(
            Process.run_id == run.id,
            Operation.log.isnot(None),
            Operation.log != ''
        ).first() is not None

        if has_db_logs:
            inferred = StorageMode.LOCAL
            if persist:
                self._persist_inferred_mode(run, inferred)
            return inferred

        # Step 3: どちらにもデータがない場合
        return StorageMode.UNKNOWN

    def _persist_inferred_mode(self, run, mode: StorageMode):
        """推論結果をDBに永続化する"""
        try:
            run.storage_mode = mode.value
            self._db.commit()
            logger.info(f"Persisted inferred storage_mode for Run {run.id}: {mode.value}")
        except Exception as e:
            logger.warning(f"Failed to persist storage_mode for Run {run.id}: {e}")
            self._db.rollback()

    def batch_infer_storage_modes(self, runs: list) -> Dict[int, StorageMode]:
        """
        複数RunのストレージモードをバッチInfer

        パフォーマンス最適化:
        1. S3存在確認は個別に実行（list_objects APIの制限）
        2. DBクエリをIN句で一括取得

        Args:
            runs: Runエンティティのリスト

        Returns:
            run_id -> StorageMode のマッピング
        """
        from define_db.models import Operation, Process

        result: Dict[int, StorageMode] = {}
        runs_to_infer = []

        # Step 1: キャッシュチェック & 明示設定済みの除外
        for run in runs:
            if run.storage_mode:
                result[run.id] = StorageMode.from_string(run.storage_mode)
            elif run.id in self._inferred_mode_cache:
                result[run.id] = self._inferred_mode_cache[run.id]
            else:
                runs_to_infer.append(run)

        if not runs_to_infer:
            return result

        # Step 2: S3一括確認（各prefixを確認）
        s3_exists: Dict[int, bool] = {}
        try:
            s3_backend = self._get_s3_backend()
            for run in runs_to_infer:
                prefix = run.storage_address or f"runs/{run.id}/"
                try:
                    res = s3_backend.list_objects_with_dirs(prefix)
                    s3_exists[run.id] = bool(res.get('contents', []))
                except Exception:
                    s3_exists[run.id] = False
        except Exception as e:
            logger.warning(f"S3 batch check failed: {e}")
            for run in runs_to_infer:
                s3_exists[run.id] = False

        # Step 3: DB一括確認（IN句で効率化）
        run_ids = [r.id for r in runs_to_infer]
        db_log_exists = set(
            row[0] for row in self._db.query(Process.run_id).join(Operation).filter(
                Process.run_id.in_(run_ids),
                Operation.log.isnot(None),
                Operation.log != ''
            ).distinct()
        )

        # Step 4: モード判定 & キャッシュ更新
        for run in runs_to_infer:
            if s3_exists.get(run.id):
                mode = StorageMode.S3
            elif run.id in db_log_exists:
                mode = StorageMode.LOCAL
            else:
                mode = StorageMode.UNKNOWN

            result[run.id] = mode
            self._inferred_mode_cache[run.id] = mode

            # UNKNOWNでなければDBに永続化
            if mode != StorageMode.UNKNOWN:
                self._persist_inferred_mode(run, mode)

        logger.info(f"Batch inferred storage_modes for {len(runs_to_infer)} runs")
        return result

    def _get_file_backend(self, mode: StorageMode):
        """モードに対応するファイルバックエンドを取得（レジストリパターン）"""
        return self._get_backend(mode.value)

    def list_contents(self, run_id: int, prefix: str = "") -> List[ContentItem]:
        """
        Run内のコンテンツ一覧を取得

        ファイルとDBデータを統合して仮想ファイルシステムとして返す。

        Args:
            run_id: Run ID
            prefix: フィルタリング用プレフィックス

        Returns:
            ContentItemのリスト
        """
        run = self._get_run(run_id)
        mode = self._get_storage_mode(run)
        items = []

        if mode == StorageMode.S3:
            # S3モード: ファイルベースのみ
            items.extend(self._list_file_contents(run, prefix))

        elif mode == StorageMode.LOCAL:
            # ローカルモード: DBからデータを仮想ファイルとして構築

            # 1. 仮想ディレクトリを追加
            items.extend(self._db_backend.list_virtual_directories(run_id, prefix))

            # 2. オペレーションログを仮想ファイルとして追加
            items.extend(self._db_backend.list_operation_logs(run_id, prefix))

            # 3. ローカルファイルシステムにもファイルがあれば追加
            items.extend(self._list_file_contents(run, prefix))

        elif mode == StorageMode.UNKNOWN:
            # UNKNOWNモード: 両方のストレージを試行（フォールバック）
            logger.info(f"Run {run_id} has unknown storage_mode, trying both S3 and local")

            # S3から試行
            s3_items = self._try_list_from_s3(run, prefix)
            items.extend(s3_items)

            # ローカル（DB + ファイル）から試行
            local_items = self._try_list_from_local(run_id, run, prefix)
            items.extend(local_items)

        # 重複除去（pathをキーとして）
        seen_paths = set()
        unique_items = []
        for item in items:
            if item.path not in seen_paths:
                seen_paths.add(item.path)
                unique_items.append(item)

        return unique_items

    def _try_list_from_s3(self, run, prefix: str = "") -> List[ContentItem]:
        """S3からコンテンツ一覧取得を試行（エラーは握りつぶす）"""
        try:
            file_backend = self._get_s3_backend()
            full_prefix = f"{run.storage_address}{prefix}"
            result = file_backend.list_objects_with_dirs(full_prefix)

            items = []
            # ファイルを変換
            for file_info in result.get('contents', []):
                key = file_info.get('Key', '')
                relative_path = key[len(run.storage_address):] if key.startswith(run.storage_address) else key
                if not relative_path or relative_path.endswith('/'):
                    continue
                name = relative_path.split('/')[-1]
                items.append(ContentItem(
                    name=name,
                    path=relative_path,
                    type="file",
                    size=file_info.get('Size', 0),
                    last_modified=file_info.get('LastModified').isoformat() if file_info.get('LastModified') else None,
                    content_type=self._detect_content_type(relative_path),
                    source=DataSource.FILE,
                    backend="s3"
                ))

            # ディレクトリを変換
            for dir_info in result.get('common_prefixes', []):
                dir_path = dir_info.get('Prefix', '')
                relative_path = dir_path[len(run.storage_address):] if dir_path.startswith(run.storage_address) else dir_path
                if not relative_path:
                    continue
                name = relative_path.rstrip('/').split('/')[-1]
                items.append(ContentItem(
                    name=name,
                    path=relative_path,
                    type="directory",
                    size=0,
                    last_modified=None,
                    content_type=ContentType.OTHER,
                    source=DataSource.FILE,
                    backend="s3"
                ))

            return items
        except Exception as e:
            logger.debug(f"S3 fallback failed for run {run.id}: {e}")
            return []

    def _try_list_from_local(self, run_id: int, run, prefix: str = "") -> List[ContentItem]:
        """ローカル（DB + ファイル）からコンテンツ一覧取得を試行（エラーは握りつぶす）"""
        items = []
        try:
            # DBから仮想ディレクトリとログを取得（backendを設定）
            db_dirs = self._db_backend.list_virtual_directories(run_id, prefix)
            for item in db_dirs:
                item.backend = "local"
            items.extend(db_dirs)

            db_logs = self._db_backend.list_operation_logs(run_id, prefix)
            for item in db_logs:
                item.backend = "local"
            items.extend(db_logs)
        except Exception as e:
            logger.debug(f"DB fallback failed for run {run_id}: {e}")

        try:
            # ローカルファイルシステムからも試行
            file_backend = self._get_local_backend()
            full_prefix = f"{run.storage_address}{prefix}"
            result = file_backend.list_objects_with_dirs(full_prefix)

            for file_info in result.get('contents', []):
                key = file_info.get('Key', '')
                relative_path = key[len(run.storage_address):] if key.startswith(run.storage_address) else key
                if not relative_path or relative_path.endswith('/'):
                    continue
                name = relative_path.split('/')[-1]
                items.append(ContentItem(
                    name=name,
                    path=relative_path,
                    type="file",
                    size=file_info.get('Size', 0),
                    last_modified=file_info.get('LastModified').isoformat() if file_info.get('LastModified') else None,
                    content_type=self._detect_content_type(relative_path),
                    source=DataSource.FILE,
                    backend="local"
                ))

            for dir_info in result.get('common_prefixes', []):
                dir_path = dir_info.get('Prefix', '')
                relative_path = dir_path[len(run.storage_address):] if dir_path.startswith(run.storage_address) else dir_path
                if not relative_path:
                    continue
                name = relative_path.rstrip('/').split('/')[-1]
                items.append(ContentItem(
                    name=name,
                    path=relative_path,
                    type="directory",
                    size=0,
                    last_modified=None,
                    content_type=ContentType.OTHER,
                    source=DataSource.FILE,
                    backend="local"
                ))
        except Exception as e:
            logger.debug(f"Local file fallback failed for run {run_id}: {e}")

        return items

    def _list_file_contents(self, run, prefix: str = "") -> List[ContentItem]:
        """ファイルバックエンドからコンテンツ一覧を取得"""
        mode = self._get_storage_mode(run)
        file_backend = self._get_file_backend(mode)
        backend_name = mode.value  # "s3" or "local"

        # 完全プレフィックスを構築
        full_prefix = f"{run.storage_address}{prefix}"

        try:
            result = file_backend.list_objects_with_dirs(full_prefix)
        except Exception as e:
            logger.warning(f"Failed to list files for {full_prefix}: {e}")
            return []

        items = []

        # ファイルを変換
        for file_info in result.get('contents', []):
            key = file_info.get('Key', '')
            # storage_addressを除去して相対パスに
            relative_path = key[len(run.storage_address):] if key.startswith(run.storage_address) else key

            if not relative_path or relative_path.endswith('/'):
                continue

            name = relative_path.split('/')[-1]
            items.append(ContentItem(
                name=name,
                path=relative_path,
                type="file",
                size=file_info.get('Size', 0),
                last_modified=file_info.get('LastModified').isoformat() if file_info.get('LastModified') else None,
                content_type=self._detect_content_type(relative_path),
                source=DataSource.FILE,
                backend=backend_name
            ))

        # ディレクトリを変換
        for dir_info in result.get('common_prefixes', []):
            dir_path = dir_info.get('Prefix', '')
            # storage_addressを除去して相対パスに
            relative_path = dir_path[len(run.storage_address):] if dir_path.startswith(run.storage_address) else dir_path

            if not relative_path:
                continue

            name = relative_path.rstrip('/').split('/')[-1]
            items.append(ContentItem(
                name=name,
                path=relative_path,
                type="directory",
                size=0,
                last_modified=None,
                content_type=ContentType.OTHER,
                source=DataSource.FILE,
                backend=backend_name
            ))

        return items

    def load_content(self, run_id: int, path: str) -> Optional[bytes]:
        """
        コンテンツを読み込む

        パスからデータソースを判定し、適切な方法で読み込む。

        Args:
            run_id: Run ID
            path: 仮想パス

        Returns:
            ファイル内容のバイト列（なければNone）
        """
        run = self._get_run(run_id)
        mode = self._get_storage_mode(run)

        # UNKNOWNモード: 両方のストレージを試行
        if mode == StorageMode.UNKNOWN:
            logger.info(f"Run {run_id} has unknown storage_mode, trying fallback for load_content")

            # まずDBからオペレーションログを試行
            if self._db_backend.is_operation_log_path(path):
                op_id = self._db_backend.extract_operation_id(path)
                if op_id:
                    content = self._db_backend.load_operation_log(op_id)
                    if content:
                        return content

            # S3から試行
            content = self._try_load_from_s3(run, path)
            if content:
                return content

            # ローカルファイルから試行
            content = self._try_load_from_local(run, path)
            if content:
                return content

            return None

        # オペレーションログの場合
        if mode == StorageMode.LOCAL and self._db_backend.is_operation_log_path(path):
            op_id = self._db_backend.extract_operation_id(path)
            if op_id:
                content = self._db_backend.load_operation_log(op_id)
                if content:
                    return content

        # ファイルバックエンドから取得
        file_backend = self._get_file_backend(mode)
        full_path = f"{run.storage_address}{path}"

        try:
            return file_backend.load(full_path)
        except Exception as e:
            logger.warning(f"Failed to load content from {full_path}: {e}")
            return None

    def _try_load_from_s3(self, run, path: str) -> Optional[bytes]:
        """S3からコンテンツ読み込みを試行（エラーは握りつぶす）"""
        try:
            file_backend = self._get_s3_backend()
            full_path = f"{run.storage_address}{path}"
            return file_backend.load(full_path)
        except Exception as e:
            logger.debug(f"S3 load fallback failed for run {run.id}, path {path}: {e}")
            return None

    def _try_load_from_local(self, run, path: str) -> Optional[bytes]:
        """ローカルファイルからコンテンツ読み込みを試行（エラーは握りつぶす）"""
        try:
            file_backend = self._get_local_backend()
            full_path = f"{run.storage_address}{path}"
            return file_backend.load(full_path)
        except Exception as e:
            logger.debug(f"Local load fallback failed for run {run.id}, path {path}: {e}")
            return None

    def get_download_url(self, run_id: int, path: str) -> str:
        """
        ダウンロードURLを取得

        S3: presigned URL
        ローカル(ファイル): /api/storage/download-direct
        ローカル(DB): /api/v2/storage/db-content

        Args:
            run_id: Run ID
            path: 仮想パス

        Returns:
            ダウンロードURL
        """
        run = self._get_run(run_id)
        mode = self._get_storage_mode(run)

        # UNKNOWNモード: オペレーションログならDB経由、それ以外は直接ダウンロード
        if mode == StorageMode.UNKNOWN:
            if self._db_backend.is_operation_log_path(path):
                op_id = self._db_backend.extract_operation_id(path)
                return f"/api/v2/storage/db-content/{run_id}?path={path}&op_id={op_id}"
            # S3のpresigned URLを試行
            try:
                s3_backend = self._get_s3_backend()
                full_path = f"{run.storage_address}{path}"
                url = s3_backend.generate_presigned_url(full_path)
                if url:
                    return url
            except Exception as e:
                logger.debug(f"S3 presigned URL fallback failed for run {run_id}: {e}")
            # フォールバック: 直接ダウンロードAPI
            return f"/api/storage/download-direct?path={run.storage_address}{path}"

        # ローカルモード + オペレーションログ → DB経由
        if mode == StorageMode.LOCAL and self._db_backend.is_operation_log_path(path):
            op_id = self._db_backend.extract_operation_id(path)
            return f"/api/v2/storage/db-content/{run_id}?path={path}&op_id={op_id}"

        # S3モード → presigned URL
        if mode == StorageMode.S3:
            file_backend = self._get_file_backend(mode)
            full_path = f"{run.storage_address}{path}"
            try:
                url = file_backend.generate_presigned_url(full_path)
                if url:
                    return url
            except Exception as e:
                logger.warning(f"Failed to generate presigned URL: {e}")

        # フォールバック: 直接ダウンロードAPI
        return f"/api/storage/download-direct?path={run.storage_address}{path}"

    def get_storage_info(self, run_id: int) -> StorageInfo:
        """
        Run固有のストレージ情報を取得

        storage_mode=nullの場合は推論を行い、適切なモードを返す。
        ハイブリッドモード（S3+DB両方にデータあり）も検出する。

        Args:
            run_id: Run ID

        Returns:
            StorageInfo
        """
        run = self._get_run(run_id)
        raw_mode = self._get_storage_mode(run)

        # storage_mode=nullの場合は推論を実行
        if raw_mode == StorageMode.UNKNOWN:
            inferred_mode = self._infer_storage_mode(run)
            mode = inferred_mode
            is_inferred = True
        else:
            mode = raw_mode
            is_inferred = False

        # ハイブリッドモードの検出
        has_s3_data = False
        has_local_data = False
        s3_path = None
        local_path = None

        # S3にデータがあるか確認
        try:
            s3_backend = self._get_s3_backend()
            bucket_name = getattr(s3_backend, 'bucket_name', 'labcode-dev-artifacts')
            storage_address = run.storage_address or f"runs/{run_id}/"
            result = s3_backend.list_objects_with_dirs(storage_address)
            has_s3_data = bool(result.get('contents', []))
            if has_s3_data:
                s3_path = f"s3://{bucket_name}/{storage_address}"
        except Exception as e:
            logger.debug(f"S3 check for hybrid failed for Run {run_id}: {e}")

        # ローカル（DB）にデータがあるか確認
        from define_db.models import Operation, Process
        has_local_data = self._db.query(Operation).join(Process).filter(
            Process.run_id == run_id,
            Operation.log.isnot(None),
            Operation.log != ''
        ).first() is not None
        if has_local_data:
            local_path = f"db://sqlite/runs/{run_id}/"

        is_hybrid = has_s3_data and has_local_data

        # アクセス可能性を判定
        is_accessible = has_s3_data or has_local_data

        # ハイブリッドモードの場合はStorageMode.HYBRIDを使用
        if is_hybrid:
            mode = StorageMode.HYBRID

        if mode == StorageMode.UNKNOWN:
            # 推論してもUNKNOWNの場合: 警告付きで返却
            return StorageInfo(
                mode=mode,
                storage_address=run.storage_address or f"runs/{run_id}/",
                full_path="unknown://",
                data_sources={
                    "logs": "unknown",
                    "yaml": "unknown",
                    "data": "unknown"
                },
                warning="Storage mode is not set and could not be inferred. Data may not be displayed correctly.",
                is_hybrid=is_hybrid,
                is_accessible=is_accessible,
                s3_path=s3_path,
                local_path=local_path
            )
        elif mode == StorageMode.HYBRID:
            # ハイブリッドモード: S3とDBの両方にデータあり
            full_path = f"hybrid://{run.storage_address or f'runs/{run_id}/'}"
            data_sources = {
                "logs": "hybrid",
                "yaml": "s3",
                "data": "hybrid"
            }
        elif mode == StorageMode.S3:
            full_path = s3_path or f"s3://labcode-dev-artifacts/{run.storage_address}"
            data_sources = {
                "logs": "s3",
                "yaml": "s3",
                "data": "s3"
            }
        else:
            full_path = local_path or f"db://sqlite/runs/{run_id}/"
            data_sources = {
                "logs": "database",
                "yaml": "database_or_none",
                "data": "database_or_none"
            }

        return StorageInfo(
            mode=mode,
            storage_address=run.storage_address or f"runs/{run_id}/",
            full_path=full_path,
            data_sources=data_sources,
            inferred=is_inferred,
            is_hybrid=is_hybrid,
            is_accessible=is_accessible,
            s3_path=s3_path,
            local_path=local_path
        )

    def _detect_content_type(self, path: str) -> ContentType:
        """パスからコンテンツ種別を判定"""
        if self._db_backend.is_operation_log_path(path):
            return ContentType.OPERATION_LOG
        elif path.endswith("protocol.yaml") or path.endswith("protocol.yml"):
            return ContentType.PROTOCOL_YAML
        elif path.endswith("manipulate.yaml") or path.endswith("manipulate.yml"):
            return ContentType.MANIPULATE_YAML
        elif path.endswith(".yaml") or path.endswith(".yml"):
            return ContentType.OTHER
        elif "processes/" in path:
            return ContentType.PROCESS_DATA
        else:
            return ContentType.OTHER
