#!/usr/bin/env python3
"""
input/outputプロセスのポート型更新スクリプト

問題:
- inputプロセスのoutputポートがUnknown
- outputプロセスのinputポートがUnknown

正しい型（protocol.yamlより）:
- inputプロセス:
  - outputポート: 接続先に応じて型を推測
    - volumeに接続 → Array[Float]
    - channelに接続 → Integer
    - その他 → 接続先の型を参照
- outputプロセス:
  - inputポート: 接続元に応じて型を推測
    - dataから接続 → Spread[Array[Float]]
    - その他 → 接続元の型を参照

戦略:
1. 接続情報から推測して型を決定
2. 推測できない場合はデフォルト型を使用
"""

import sys
from pathlib import Path

sys.path.append("/app")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from define_db.models import Port, PortConnection, Process

DB_PATH = "/data/sql_app.db"
engine = create_engine(f"sqlite:///{DB_PATH}")
SessionLocal = sessionmaker(bind=engine)


def update_io_port_types():
    """input/outputプロセスのポート型を更新"""
    session = SessionLocal()

    try:
        print("=== input/outputプロセスのポート型更新 ===\n")

        updated_count = 0

        # inputプロセスのoutputポートを更新
        print("--- inputプロセスのoutputポート ---\n")
        input_processes = session.query(Process).filter(
            Process.name == "input"
        ).all()

        for process in input_processes:
            # outputポートを取得
            output_port = session.query(Port).filter(
                Port.process_id == process.id,
                Port.port_name == "output",
                Port.port_type == "output"
            ).first()

            if not output_port:
                print(f"Run {process.run_id}: outputポートが見つかりません\n")
                continue

            print(f"Run {process.run_id}, Process 'input' (ID: {process.id}):")
            print(f"  - output (ID: {output_port.id}): {output_port.data_type}")

            # 接続先を確認して型を推測
            connections = session.query(PortConnection).filter(
                PortConnection.source_port_id == output_port.id
            ).all()

            inferred_type = None
            if connections:
                # 最初の接続先を確認
                first_conn = connections[0]
                target_port = session.query(Port).filter(
                    Port.id == first_conn.target_port_id
                ).first()

                if target_port:
                    target_process = session.query(Process).filter(
                        Process.id == target_port.process_id
                    ).first()

                    print(f"    接続先: {target_process.name if target_process else 'Unknown'}.{target_port.port_name}")

                    # ポート名から型を推測
                    if target_port.port_name == "volume":
                        inferred_type = "Array[Float]"
                    elif target_port.port_name == "channel":
                        inferred_type = "Integer"
                    elif target_port.data_type and target_port.data_type != "Unknown":
                        # ターゲットポートの型を使用
                        inferred_type = target_port.data_type

            if not inferred_type:
                # デフォルトでArray[Float]を使用（volumeが最も一般的）
                inferred_type = "Array[Float]"
                print(f"    接続なし、デフォルト型を使用")

            # 型を更新
            if output_port.data_type != inferred_type:
                print(f"    → 型を更新: {output_port.data_type} → {inferred_type}\n")
                output_port.data_type = inferred_type
                updated_count += 1
            else:
                print(f"    → すでに正しい型です\n")

        # outputプロセスのinputポートを更新
        print("\n--- outputプロセスのinputポート ---\n")
        output_processes = session.query(Process).filter(
            Process.name == "output"
        ).all()

        for process in output_processes:
            # inputポートを取得
            input_port = session.query(Port).filter(
                Port.process_id == process.id,
                Port.port_name == "input",
                Port.port_type == "input"
            ).first()

            if not input_port:
                print(f"Run {process.run_id}: inputポートが見つかりません\n")
                continue

            print(f"Run {process.run_id}, Process 'output' (ID: {process.id}):")
            print(f"  - input (ID: {input_port.id}): {input_port.data_type}")

            # 接続元を確認して型を推測
            connections = session.query(PortConnection).filter(
                PortConnection.target_port_id == input_port.id
            ).all()

            inferred_type = None
            if connections:
                # 最初の接続元を確認
                first_conn = connections[0]
                source_port = session.query(Port).filter(
                    Port.id == first_conn.source_port_id
                ).first()

                if source_port:
                    source_process = session.query(Process).filter(
                        Process.id == source_port.process_id
                    ).first()

                    print(f"    接続元: {source_process.name if source_process else 'Unknown'}.{source_port.port_name}")

                    # ソースポートの型を使用
                    if source_port.data_type and source_port.data_type != "Unknown":
                        inferred_type = source_port.data_type

            if not inferred_type:
                # デフォルトでSpread[Array[Float]]を使用（protocol.yamlのoutput.data）
                inferred_type = "Spread[Array[Float]]"
                print(f"    接続なし、デフォルト型を使用")

            # 型を更新
            if input_port.data_type != inferred_type:
                print(f"    → 型を更新: {input_port.data_type} → {inferred_type}\n")
                input_port.data_type = inferred_type
                updated_count += 1
            else:
                print(f"    → すでに正しい型です\n")

        # コミット
        session.commit()

        print("=" * 60)
        print(f"✅ 更新完了:")
        print(f"   - 更新したポート: {updated_count}個")
        print("=" * 60)

        # 検証
        print("\n=== 検証: 残りのUnknownポート ===")
        remaining_unknown = session.query(Port).join(Process).filter(
            Process.name.in_(["input", "output"]),
            Port.data_type == "Unknown"
        ).count()

        if remaining_unknown > 0:
            print(f"⚠️  警告: まだ{remaining_unknown}個のUnknownポートが残っています")
        else:
            print("✅ すべてのinput/outputポートが更新されました")

    except Exception as e:
        session.rollback()
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    update_io_port_types()
