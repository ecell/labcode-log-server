#!/usr/bin/env python3
"""
マイグレーションスクリプト: Google Drive URL → S3パス

既存RunデータのGoogle Drive URLをS3パス形式に変換する。

使用方法:
    Docker内で実行:
    docker exec labcode_log_server python /app/scripts/migrate_storage_address.py [--dry-run]

    直接実行（プロジェクトルートから）:
    cd labcode-log-server/scripts
    python migrate_storage_address.py [--dry-run]

オプション:
    --dry-run  実際に更新せず、対象レコードを表示するだけ

作成日: 2025-12-21
作成者: Astra エージェント
"""

import sys
import argparse
from pathlib import Path

# プロジェクトルートをパスに追加（直接実行時用）
app_dir = Path(__file__).parent.parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

try:
    from define_db.database import SessionLocal
    from define_db.models import Run
except ImportError:
    # Docker内で実行する場合
    sys.path.insert(0, '/app')
    from define_db.database import SessionLocal
    from define_db.models import Run


def migrate_storage_address(dry_run: bool = False):
    """Google Drive URLをS3パスに移行"""

    print("=" * 60)
    print("Storage Address Migration: Google Drive URL → S3 Path")
    print("=" * 60)

    with SessionLocal() as session:
        # Google Drive URLを持つRunを検索
        runs_with_url = session.query(Run).filter(
            Run.storage_address.like('https://drive.google.com%')
        ).all()

        print(f"\n対象レコード数: {len(runs_with_url)}")

        if not runs_with_url:
            print("✅ 移行対象のレコードはありません。")
            return

        print("\n移行対象:")
        print("-" * 60)

        for run in runs_with_url:
            old_value = run.storage_address
            new_value = f"runs/{run.id}/"

            print(f"  Run ID: {run.id}")
            print(f"    旧: {old_value[:50]}...")
            print(f"    新: {new_value}")
            print()

            if not dry_run:
                run.storage_address = new_value

        if dry_run:
            print("-" * 60)
            print("🔍 [DRY RUN] 実際の更新は行われませんでした。")
            print("    実行するには --dry-run オプションを外してください。")
        else:
            session.commit()
            print("-" * 60)
            print(f"✅ {len(runs_with_url)} 件のレコードを更新しました。")

        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Google Drive URLをS3パスに移行"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='実際に更新せず、対象レコードを表示するだけ'
    )

    args = parser.parse_args()
    migrate_storage_address(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
