"""
Processテーブルにprocess_typeカラムを追加するマイグレーションスクリプト

実行方法:
    # Dockerコンテナ内で実行
    docker exec -it labcode_log_server python scripts/add_process_type_column.py

    # ローカルで実行
    cd labcode-test-environment/labcode-log-server
    python scripts/add_process_type_column.py
"""
import sys
from pathlib import Path

# パス調整
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

from sqlalchemy import text
from define_db.database import engine, SessionLocal
from define_db.models import Process, Run
import yaml


def add_process_type_column():
    """Step 1: process_typeカラムをProcessテーブルに追加"""
    print("=== Step 1: Adding process_type column to processes table ===")

    with engine.connect() as conn:
        # カラム追加
        try:
            conn.execute(text("""
                ALTER TABLE processes
                ADD COLUMN process_type VARCHAR(256)
            """))
            conn.commit()
            print("✅ Column 'process_type' added successfully")
        except Exception as e:
            if "duplicate column name" in str(e).lower():
                print("⚠️  Column 'process_type' already exists, skipping")
            else:
                raise


def migrate_existing_data():
    """Step 2: 既存Processレコードのprocess_typeを埋める"""
    print("\n=== Step 2: Migrating existing process_type data ===")

    with SessionLocal() as session:
        # 全Process取得
        processes = session.query(Process).all()

        total = len(processes)
        updated = 0
        skipped = 0
        errors = 0

        print(f"Found {total} processes to migrate")

        for process in processes:
            # 既にprocess_typeが設定されている場合はスキップ
            if process.process_type:
                skipped += 1
                continue

            try:
                # Runを取得
                run = session.query(Run).filter(Run.id == process.run_id).first()
                if not run or not run.storage_address:
                    print(f"  Process {process.id}: No storage_address, skipping")
                    skipped += 1
                    continue

                # storage_addressがURLの場合はスキップ
                if run.storage_address.startswith('http'):
                    print(f"  Process {process.id}: Remote URL, skipping")
                    skipped += 1
                    continue

                # protocol.yamlから読み込み
                protocol_path = Path(run.storage_address) / "protocol.yaml"
                if not protocol_path.exists():
                    print(f"  Process {process.id}: protocol.yaml not found at {protocol_path}")
                    skipped += 1
                    continue

                with open(protocol_path, 'r', encoding='utf-8') as f:
                    protocol_data = yaml.safe_load(f)

                # process.nameとoperations.idをマッチング
                process_type = None
                for op in protocol_data.get('operations', []):
                    if op.get('id') == process.name:
                        process_type = op.get('type')
                        break

                if process_type:
                    process.process_type = process_type
                    updated += 1
                    print(f"  Process {process.id} ({process.name}): {process_type}")
                else:
                    print(f"  Process {process.id}: Type not found in protocol.yaml")
                    skipped += 1

            except Exception as e:
                print(f"  Process {process.id}: Error - {e}")
                errors += 1

        # コミット
        session.commit()

        print(f"\n=== Migration Summary ===")
        print(f"Total processes: {total}")
        print(f"Updated: {updated}")
        print(f"Skipped: {skipped}")
        print(f"Errors: {errors}")


def verify_migration():
    """Step 3: マイグレーション結果を検証"""
    print("\n=== Step 3: Verifying migration ===")

    with SessionLocal() as session:
        # process_typeがNULLのレコード数
        null_count = session.query(Process).filter(
            Process.process_type.is_(None)
        ).count()

        # process_typeが設定されているレコード数
        set_count = session.query(Process).filter(
            Process.process_type.isnot(None)
        ).count()

        total = session.query(Process).count()

        print(f"Total processes: {total}")
        print(f"With process_type: {set_count}")
        print(f"Without process_type (NULL): {null_count}")

        if null_count > 0:
            print("\n⚠️  Some processes still have NULL process_type")
            print("This is expected if:")
            print("  - storage_address is a remote URL")
            print("  - protocol.yaml is missing")
            print("  - Process name doesn't match operations.id in YAML")
        else:
            print("\n✅ All processes have process_type set")


if __name__ == "__main__":
    print("Starting process_type migration...\n")

    # Step 1: カラム追加
    add_process_type_column()

    # Step 2: 既存データ移行
    migrate_existing_data()

    # Step 3: 検証
    verify_migration()

    print("\n✅ Migration completed successfully")
