#!/usr/bin/env python3
"""
既存のUnknownポート型を正しい型に更新するスクリプト

manipulate.yamlから型定義を読み込み、
data_type="Unknown"となっているポートを正しい型に更新します。
"""

from define_db.database import SessionLocal
from define_db.models import Port, Process
from services.port_type_mapper import get_port_type_mapper


def update_unknown_port_types():
    """data_type="Unknown"のポートを正しい型に更新"""

    type_mapper = get_port_type_mapper()

    with SessionLocal() as session:
        # data_type="Unknown"のすべてのポートを取得
        unknown_ports = session.query(Port).filter(Port.data_type == "Unknown").all()

        total_ports = len(unknown_ports)
        updated_count = 0
        skipped_count = 0

        print(f"{'='*60}")
        print(f"Port Type Update - Unknown → Actual Type")
        print(f"{'='*60}")
        print(f"Found {total_ports} ports with 'Unknown' type\n")

        for port in unknown_ports:
            # プロセス情報を取得
            process = session.query(Process).filter(Process.id == port.process_id).first()

            if not process or not process.process_type:
                print(f"Port {port.id:4d} ({port.port_name:20s}): ⏭️  Process type not available")
                skipped_count += 1
                continue

            # manipulate.yamlから型を取得
            port_type = type_mapper.get_port_type(
                process.process_type,
                port.port_name,
                port.port_type
            )

            if port_type == "Unknown":
                # YAML定義から型を推測できない場合、全ポート定義から推測
                all_ports = type_mapper.get_all_ports_for_process(process.process_type)
                if all_ports:
                    ports_def = all_ports.get(port.port_type, [])
                    if port.position < len(ports_def):
                        port_def = ports_def[port.position]
                        port_type = port_def.get('type', 'Unknown')
                        # ポート名も更新
                        correct_port_name = port_def.get('id')
                        if correct_port_name and correct_port_name != port.port_name:
                            old_name = port.port_name
                            port.port_name = correct_port_name
                            print(f"Port {port.id:4d}: Renamed {old_name} → {correct_port_name}")

            if port_type != "Unknown":
                old_type = port.data_type
                port.data_type = port_type
                print(f"Port {port.id:4d} ({process.name:15s}.{port.port_name:15s}): {old_type:15s} → {port_type}")
                updated_count += 1
            else:
                print(f"Port {port.id:4d} ({process.name:15s}.{port.port_name:15s}): ⏭️  Type not found in YAML")
                skipped_count += 1

        # 変更をコミット
        session.commit()

        print(f"\n{'='*60}")
        print(f"Summary:")
        print(f"  Total Ports:       {total_ports}")
        print(f"  Updated:           {updated_count}")
        print(f"  Skipped:           {skipped_count}")
        print(f"{'='*60}")


if __name__ == "__main__":
    update_unknown_port_types()
