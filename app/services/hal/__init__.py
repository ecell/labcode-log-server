"""Hybrid Access Layer (HAL)

S3/ローカルファイルとDBデータを統一的に扱うアクセス抽象化レイヤー。
Run.storage_modeとデータ種別に基づいて適切なデータソースを選択。

使用例:
    from services.hal import HybridAccessLayer

    hal = HybridAccessLayer(db_session)
    items = hal.list_contents(run_id=22)
    content = hal.load_content(run_id=22, path="operations/172/log.txt")

    # Runリストのstorage_mode推論（キャッシュ付き）
    from services.hal import infer_storage_mode_for_run
    inferred_mode = infer_storage_mode_for_run(db_session, run)

    # バッチ推論（Run List用最適化）
    from services.hal import batch_infer_storage_modes
    batch_infer_storage_modes(db_session, runs)
"""

import logging
from typing import List, Set

from .models import StorageMode, ContentType, DataSource, ContentItem, StorageInfo
from .hybrid_access_layer import HybridAccessLayer

logger = logging.getLogger(__name__)


def infer_storage_mode_for_run(db_session, run, persist: bool = True) -> str:
    """
    Runのstorage_modeを推論して文字列で返す（キャッシュ付き）

    storage_mode=nullの場合は、DBログの有無やS3ファイルの有無から推論。
    S3とDBの両方にデータがある場合は'hybrid'を返す。
    明示的に設定されている場合はそのまま返す（キャッシュヒット）。

    推論結果はDBに永続化され、次回以降はS3/DBアクセスなしで取得可能。

    Args:
        db_session: SQLAlchemy Session
        run: Runエンティティ
        persist: 推論結果をDBに永続化するかどうか（デフォルト: True）

    Returns:
        str: 's3', 'local', 'hybrid', or 'unknown'
    """
    # キャッシュヒット: storage_modeが既に設定されている場合
    if run.storage_mode is not None:
        return run.storage_mode

    # キャッシュミス: 推論を実行
    hal = HybridAccessLayer(db_session)

    # get_storage_infoでハイブリッド判定を含めた完全な情報を取得
    storage_info = hal.get_storage_info(run.id)

    # ハイブリッドの場合は'hybrid'
    if storage_info.is_hybrid:
        inferred_mode = 'hybrid'
    else:
        # それ以外は推論されたモードを取得
        mode = hal._infer_storage_mode(run)
        inferred_mode = mode.value

    # DBに永続化（次回以降はキャッシュヒット）
    if persist:
        run.storage_mode = inferred_mode
        db_session.commit()
        logger.info(f"Persisted inferred storage_mode for Run {run.id}: {inferred_mode}")

    return inferred_mode


def batch_infer_storage_modes(db_session, runs: List) -> None:
    """
    複数Runのstorage_modeを一括推論・永続化

    最適化:
    - S3にあるRun IDを一括取得（1回のS3リクエスト）
    - DBにログがあるRun IDを一括取得（1回のDBクエリ）
    - 未キャッシュのRunのみ処理

    Args:
        db_session: SQLAlchemy Session
        runs: Runエンティティのリスト
    """
    # 未キャッシュのRunのみ抽出
    uncached_runs = [r for r in runs if r.storage_mode is None]

    if not uncached_runs:
        logger.debug("All runs have cached storage_mode, skipping batch inference")
        return

    logger.info(f"Batch inferring storage_mode for {len(uncached_runs)} runs")

    run_ids = [r.id for r in uncached_runs]

    # 一括でS3データの有無を確認
    s3_run_ids = _batch_check_s3_presence(run_ids)

    # 一括でDBログの有無を確認
    db_run_ids = _batch_check_db_logs(db_session, run_ids)

    # 各Runにモードを設定
    updated_count = 0
    for run in uncached_runs:
        has_s3 = run.id in s3_run_ids
        has_db = run.id in db_run_ids

        if has_s3 and has_db:
            run.storage_mode = 'hybrid'
        elif has_s3:
            run.storage_mode = 's3'
        elif has_db:
            run.storage_mode = 'local'
        else:
            run.storage_mode = 'unknown'

        updated_count += 1

    db_session.commit()
    logger.info(f"Batch persisted storage_mode for {updated_count} runs")


def _batch_check_s3_presence(run_ids: List[int]) -> Set[int]:
    """
    S3にデータが存在するRun IDのセットを取得

    S3のlist_objects_v2を使用して、runs/プレフィックス配下のフォルダを一括取得。

    Args:
        run_ids: チェック対象のRun IDリスト

    Returns:
        S3にデータが存在するRun IDのセット
    """
    try:
        from services.storage_service import BackendRegistry, StorageConfig

        config = StorageConfig.from_env()
        if not BackendRegistry.is_registered('s3'):
            return set()

        backend_class = BackendRegistry.get('s3')
        s3_backend = backend_class(config.s3)

        # runs/プレフィックス配下のディレクトリを一括取得
        result = s3_backend.list_objects_with_dirs("runs/")
        common_prefixes = result.get('common_prefixes', [])

        # プレフィックスからRun IDを抽出
        s3_run_ids = set()
        for prefix_info in common_prefixes:
            prefix = prefix_info.get('Prefix', '')
            # "runs/21/" -> 21
            parts = prefix.strip('/').split('/')
            if len(parts) >= 2:
                try:
                    run_id = int(parts[1])
                    if run_id in run_ids:
                        s3_run_ids.add(run_id)
                except ValueError:
                    pass

        logger.debug(f"S3 batch check: found {len(s3_run_ids)} runs with data")
        return s3_run_ids

    except Exception as e:
        logger.warning(f"S3 batch check failed: {e}")
        return set()


def _batch_check_db_logs(db_session, run_ids: List[int]) -> Set[int]:
    """
    DBにログが存在するRun IDのセットを取得

    1回のDBクエリで全Run IDのログ有無を確認。

    Args:
        db_session: SQLAlchemy Session
        run_ids: チェック対象のRun IDリスト

    Returns:
        DBにログが存在するRun IDのセット
    """
    try:
        from define_db.models import Operation, Process
        from sqlalchemy import func

        # Run ID毎にOperation.logが存在するかを一括クエリ
        result = db_session.query(Process.run_id).join(Operation).filter(
            Process.run_id.in_(run_ids),
            Operation.log.isnot(None),
            Operation.log != ''
        ).distinct().all()

        db_run_ids = {row[0] for row in result}
        logger.debug(f"DB batch check: found {len(db_run_ids)} runs with logs")
        return db_run_ids

    except Exception as e:
        logger.warning(f"DB batch check failed: {e}")
        return set()


__all__ = [
    'StorageMode',
    'ContentType',
    'DataSource',
    'ContentItem',
    'StorageInfo',
    'HybridAccessLayer',
    'infer_storage_mode_for_run',
    'batch_infer_storage_modes',
]
