#!/usr/bin/env python3
"""
既存YAMLデータをPorts/PortConnectionsテーブルに移行するスクリプト

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
    """全Runのポート情報をマイグレーション"""
    with SessionLocal() as session:
        runs = session.query(Run).filter(Run.deleted_at.is_(None)).all()

        total_ports = 0
        total_connections = 0
        skipped_count = 0

        print(f"Found {len(runs)} runs to process.\n")

        for run in runs:
            print(f"Processing Run {run.id}: {run.file_name}")

            # storage_addressがGoogle Drive URLの場合はスキップ
            if run.storage_address.startswith("http"):
                print(f"  ⏭️  Skipping (Google Drive URL): {run.storage_address}")
                skipped_count += 1
                continue

            # YAMLファイル存在確認
            protocol_path = Path(run.storage_address) / "protocol.yaml"
            manipulate_path = Path(run.storage_address) / "manipulate.yaml"

            if not protocol_path.exists() or not manipulate_path.exists():
                print(f"  ⏭️  Skipping (YAML not found): {run.storage_address}")
                skipped_count += 1
                continue

            if dry_run:
                print(f"  🔍 [DRY RUN] Would import from {run.storage_address}")
                continue

            try:
                importer = YAMLPortImporter(session)
                result = importer.import_from_run(run.id, run.storage_address)
                total_ports += result['ports_created']
                total_connections += result['connections_created']
                print(f"  ✅ Ports: {result['ports_created']}, Connections: {result['connections_created']}")
            except Exception as e:
                print(f"  ❌ Error: {e}")

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Summary:")
        print(f"  Total Runs: {len(runs)}")
        print(f"  Processed: {len(runs) - skipped_count}")
        print(f"  Skipped: {skipped_count}")
        if not dry_run:
            print(f"  Ports Created: {total_ports}")
            print(f"  Connections Created: {total_connections}")


def migrate_single_run(run_id: int, dry_run: bool = False):
    """特定のRunのポート情報をマイグレーション"""
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
