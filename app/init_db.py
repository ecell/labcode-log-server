#!/usr/bin/env python3
"""
データベース自動初期化・マイグレーションモジュール

FastAPI起動時に呼び出され：
1. データベースの状態をチェック
2. 必要に応じてテーブルを自動作成（既存データは保持）
3. 必要に応じてカスタムマイグレーションを実行
"""
import logging
from pathlib import Path
from sqlalchemy import text
from define_db.database import engine
from define_db.models import Base

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DB_PATH = Path("/data/sql_app.db")

REQUIRED_TABLES = [
    'users', 'projects', 'runs', 'processes',
    'operations', 'edges', 'ports', 'port_connections',
    'process_operations'
]

# ============================================
# カスタムマイグレーション定義
# ============================================
MIGRATIONS = [
    {
        "version": "001",
        "description": "Ensure storage_mode column in runs",
        "check": "SELECT 1 FROM pragma_table_info('runs') WHERE name='storage_mode'",
        "sql": "ALTER TABLE runs ADD COLUMN storage_mode VARCHAR(10)"
    },
    {
        "version": "002",
        "description": "Ensure process_type column in processes",
        "check": "SELECT 1 FROM pragma_table_info('processes') WHERE name='process_type'",
        "sql": "ALTER TABLE processes ADD COLUMN process_type VARCHAR(256)"
    },
    {
        "version": "003",
        "description": "Ensure display_visible column in runs",
        "check": "SELECT 1 FROM pragma_table_info('runs') WHERE name='display_visible'",
        "sql": "ALTER TABLE runs ADD COLUMN display_visible BOOLEAN DEFAULT 1 NOT NULL"
    },
]


def check_database_file() -> dict:
    """データベースファイルの状態をチェック"""
    result = {
        'exists': False,
        'size': 0,
        'is_empty': True,
        'is_readable': False
    }

    if DB_PATH.exists():
        result['exists'] = True
        result['size'] = DB_PATH.stat().st_size
        result['is_empty'] = result['size'] == 0

        try:
            with open(DB_PATH, 'rb') as f:
                f.read(16)
            result['is_readable'] = True
        except (IOError, PermissionError):
            result['is_readable'] = False

    return result


def check_tables() -> dict:
    """データベース内のテーブル存在をチェック"""
    result = {
        'existing_tables': [],
        'missing_tables': [],
        'all_present': False
    }

    try:
        with engine.connect() as conn:
            query = text("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in conn.execute(query)]
            result['existing_tables'] = tables
            result['missing_tables'] = [t for t in REQUIRED_TABLES if t not in tables]
            result['all_present'] = len(result['missing_tables']) == 0
    except Exception as e:
        logger.warning(f"テーブルチェック中にエラー: {e}")
        result['missing_tables'] = REQUIRED_TABLES

    return result


def create_tables():
    """
    全テーブルを作成

    重要: create_all()は既存テーブルのデータを削除しない
    既存テーブルはスキップされ、新規テーブルのみ作成される
    """
    logger.info("テーブル作成を開始（既存テーブルはスキップ）...")
    Base.metadata.create_all(engine)
    logger.info("テーブル作成完了")


def run_custom_migrations():
    """
    カスタムマイグレーションを実行

    既存テーブルへのカラム追加など、create_all()で対応できない
    スキーマ変更を実行する。既存データは保持される。
    """
    logger.info("カスタムマイグレーションをチェック...")

    with engine.connect() as conn:
        applied_count = 0
        skipped_count = 0

        for migration in MIGRATIONS:
            version = migration["version"]
            description = migration["description"]

            try:
                result = conn.execute(text(migration["check"]))
                if result.fetchone():
                    logger.debug(f"Migration {version} already applied: {description}")
                    skipped_count += 1
                    continue
            except Exception:
                skipped_count += 1
                continue

            logger.info(f"Applying migration {version}: {description}")
            try:
                conn.execute(text(migration["sql"]))
                conn.commit()
                logger.info(f"Migration {version} completed")
                applied_count += 1
            except Exception as e:
                logger.error(f"Migration {version} failed: {e}")

        if applied_count > 0:
            logger.info(f"マイグレーション完了: {applied_count}件適用, {skipped_count}件スキップ")
        else:
            logger.info(f"マイグレーション: 全て適用済み ({skipped_count}件)")


def ensure_database_ready() -> dict:
    """
    データベースが使用可能な状態であることを保証

    Returns:
        dict: 実行結果サマリー
    """
    summary = {
        'action': None,
        'file_status': None,
        'table_status': None,
        'migrations_run': False,
        'success': False
    }

    # Step 1: ファイル状態チェック
    file_status = check_database_file()
    summary['file_status'] = file_status

    need_create = False

    if not file_status['exists']:
        logger.info(f"[DB Init] データベースファイルが存在しません: {DB_PATH}")
        need_create = True
        summary['action'] = 'create_new'

    elif file_status['is_empty']:
        logger.warning(f"[DB Init] データベースファイルが空です (0 bytes)")
        need_create = True
        summary['action'] = 'initialize_empty'

    if need_create:
        logger.info("[DB Init] テーブルを作成します...")
        create_tables()

        table_status = check_tables()
        summary['table_status'] = table_status

        if table_status['all_present']:
            logger.info("[DB Init] 初期化完了")
            summary['success'] = True
        return summary

    # Step 2: テーブル存在チェック
    table_status = check_tables()
    summary['table_status'] = table_status

    if not table_status['all_present']:
        missing = ', '.join(table_status['missing_tables'])
        logger.info(f"[DB Init] 不足テーブル: {missing}")
        logger.info("[DB Init] 不足テーブルを作成します（既存データは保持）...")
        summary['action'] = 'create_missing'

        create_tables()

        table_status = check_tables()
        summary['table_status'] = table_status

    # Step 3: カスタムマイグレーション
    run_custom_migrations()
    summary['migrations_run'] = True

    if summary['action'] is None:
        summary['action'] = 'none'

    summary['success'] = True
    logger.info(f"[DB Init] データベース準備完了 (テーブル数: {len(table_status['existing_tables'])})")

    return summary


if __name__ == "__main__":
    result = ensure_database_ready()
    print(f"\n=== 実行結果 ===")
    print(f"アクション: {result['action']}")
    print(f"成功: {result['success']}")
    if result['table_status']:
        print(f"テーブル数: {len(result['table_status']['existing_tables'])}")
