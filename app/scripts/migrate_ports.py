#!/usr/bin/env python3
"""
既存YAMLデータをPorts/PortConnectionsテーブルに移行するスクリプト（冪等性対応）

★冪等性: 何度実行しても安全です。既存データはスキップされます。

使用方法:
    # 全Run移行
    docker exec -it <container_id> python /app/scripts/migrate_ports.py

    # 特定Run移行
    docker exec -it <container_id> python /app/scripts/migrate_ports.py --run-id 1

    # Dry-run(実際には移行しない)
    docker exec -it <container_id> python /app/scripts/migrate_ports.py --dry-run
"""

import sys
from pathlib import Path

# app ディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

from define_db.database import SessionLocal
from define_db.models import Run
from services.yaml_importer import YAMLPortImporter
import argparse


def migrate_all_runs(dry_run: bool = False):
    """全Runのポート情報をマイグレーション（冪等性対応）"""
    with SessionLocal() as session:
        runs = session.query(Run).filter(Run.deleted_at.is_(None)).all()

        total_ports_created = 0
        total_ports_skipped = 0
        total_connections_created = 0
        total_connections_skipped = 0
        run_skipped_count = 0

        print(f"Found {len(runs)} runs to process.\n")

        for run in runs:
            print(f"Processing Run {run.id}: {run.file_name}")

            # storage_addressがGoogle Drive URLの場合はスキップ
            if run.storage_address.startswith("http"):
                print(f"  ⏭️  Skipping (Google Drive URL): {run.storage_address}")
                run_skipped_count += 1
                continue

            # YAMLファイル存在確認
            protocol_path = Path(run.storage_address) / "protocol.yaml"
            manipulate_path = Path(run.storage_address) / "manipulate.yaml"

            if not protocol_path.exists() or not manipulate_path.exists():
                print(f"  ⏭️  Skipping (YAML not found): {run.storage_address}")
                run_skipped_count += 1
                continue

            if dry_run:
                print(f"  🔍 [DRY RUN] Would import from {run.storage_address}")
                continue

            try:
                importer = YAMLPortImporter(session)
                result = importer.import_from_run(run.id, run.storage_address)
                total_ports_created += result['ports_created']
                total_ports_skipped += result['ports_skipped']
                total_connections_created += result['connections_created']
                total_connections_skipped += result['connections_skipped']

                # 結果表示
                if result['ports_skipped'] > 0 or result['connections_skipped'] > 0:
                    print(f"  ✅ Created: {result['ports_created']} ports, {result['connections_created']} connections")
                    print(f"     Skipped: {result['ports_skipped']} ports, {result['connections_skipped']} connections (already exist)")
                else:
                    print(f"  ✅ Ports: {result['ports_created']}, Connections: {result['connections_created']}")
            except Exception as e:
                print(f"  ❌ Error: {e}")

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Summary:")
        print(f"  Total Runs: {len(runs)}")
        print(f"  Processed: {len(runs) - run_skipped_count}")
        print(f"  Skipped (no YAML/remote): {run_skipped_count}")
        if not dry_run:
            print(f"  Ports: {total_ports_created} created, {total_ports_skipped} skipped")
            print(f"  Connections: {total_connections_created} created, {total_connections_skipped} skipped")
            if total_ports_skipped > 0 or total_connections_skipped > 0:
                print(f"\n✅ This migration is idempotent - skipped items already existed.")


def migrate_single_run(run_id: int, dry_run: bool = False):
    """特定のRunのポート情報をマイグレーション（冪等性対応）"""
    with SessionLocal() as session:
        run = session.query(Run).filter(Run.id == run_id).first()
        if not run:
            print(f"Run {run_id} not found.")
            return

        print(f"Processing Run {run.id}: {run.file_name}")

        if run.storage_address.startswith("http"):
            print(f"  ⏭️  Cannot migrate (Google Drive URL): {run.storage_address}")
            return

        protocol_path = Path(run.storage_address) / "protocol.yaml"
        manipulate_path = Path(run.storage_address) / "manipulate.yaml"

        if not protocol_path.exists() or not manipulate_path.exists():
            print(f"  ⏭️  Cannot migrate (YAML not found): {run.storage_address}")
            return

        if dry_run:
            print(f"  🔍 [DRY RUN] Would import from {run.storage_address}")
            return

        try:
            importer = YAMLPortImporter(session)
            result = importer.import_from_run(run.id, run.storage_address)

            # 結果表示
            if result['ports_skipped'] > 0 or result['connections_skipped'] > 0:
                print(f"  ✅ Created: {result['ports_created']} ports, {result['connections_created']} connections")
                print(f"     Skipped: {result['ports_skipped']} ports, {result['connections_skipped']} connections (already exist)")
                print(f"\n✅ This migration is idempotent - skipped items already existed.")
            else:
                print(f"  ✅ Ports: {result['ports_created']}, Connections: {result['connections_created']}")
        except Exception as e:
            print(f"  ❌ Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate YAML port data to database")
    parser.add_argument("--run-id", type=int, help="Migrate only specified Run ID")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (don't actually migrate)")

    args = parser.parse_args()

    if args.run_id:
        migrate_single_run(args.run_id, dry_run=args.dry_run)
    else:
        migrate_all_runs(dry_run=args.dry_run)
