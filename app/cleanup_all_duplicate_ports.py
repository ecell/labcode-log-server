#!/usr/bin/env python3
"""
全プロセスタイプの重複ポートクリーンアップスクリプト

YAMLインポートとフォールバック生成の両方が実行され、重複ポートが作成された問題を修正
"""

import sys
from pathlib import Path

sys.path.append("/app")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from define_db.models import Port, PortConnection, Process
from services.port_type_mapper import get_port_type_mapper

DB_PATH = "/data/sql_app.db"
engine = create_engine(f"sqlite:///{DB_PATH}")
SessionLocal = sessionmaker(bind=engine)


def cleanup_all_duplicate_ports():
    """全プロセスタイプの重複ポートをクリーンアップ"""
    session = SessionLocal()

    try:
        # ポート型マッパーを初期化
        type_mapper = get_port_type_mapper()

        print("=== 重複ポートを持つプロセスを検出 ===\n")

        # output_2ポートを持つすべてのプロセスを取得
        processes_with_output_2 = session.query(Process).join(Port).filter(
            Port.port_name == "output_2",
            Port.port_type == "output"
        ).distinct().all()

        migrated_count = 0
        deleted_count = 0

        for process in processes_with_output_2:
            # YAMLから正しいポート名を取得
            correct_port_name = None
            if process.process_type:
                all_ports = type_mapper.get_all_ports_for_process(process.process_type)
                if all_ports and all_ports.get('output'):
                    output_ports_def = all_ports['output']
                    # 2番目の出力ポート（index 1）の定義を取得
                    if len(output_ports_def) >= 2:
                        # 2つ目のポートがあれば、それが正解
                        correct_port_name = output_ports_def[1].get('id')
                    elif len(output_ports_def) == 1:
                        # 1つしかなければ、それが正解（output_2は重複）
                        correct_port_name = output_ports_def[0].get('id')

            if not correct_port_name:
                print(f"⚠️  Run {process.run_id}, Process '{process.name}' (ID: {process.id}, Type: {process.process_type}):")
                print(f"    正しいポート名が特定できません。スキップします。\n")
                continue

            # 正しいポートとoutput_2ポートを取得
            correct_port = session.query(Port).filter(
                Port.process_id == process.id,
                Port.port_name == correct_port_name,
                Port.port_type == "output"
            ).first()

            output_2_port = session.query(Port).filter(
                Port.process_id == process.id,
                Port.port_name == "output_2",
                Port.port_type == "output"
            ).first()

            # output_2があるが、正しいポートがない場合 → output_2をリネーム
            if output_2_port and not correct_port:
                print(f"✏️  Run {process.run_id}, Process '{process.name}' (ID: {process.id}, Type: {process.process_type}):")
                print(f"    output_2 (ID: {output_2_port.id}) → {correct_port_name} にリネーム")

                # data_typeも更新
                new_data_type = "Unknown"
                if process.process_type:
                    all_ports = type_mapper.get_all_ports_for_process(process.process_type)
                    if all_ports and all_ports.get('output'):
                        output_ports_def = all_ports['output']
                        if len(output_ports_def) >= 1:
                            new_data_type = output_ports_def[0].get('type', 'Unknown')

                output_2_port.port_name = correct_port_name
                output_2_port.data_type = new_data_type
                print(f"    データ型も更新: {new_data_type}\n")
                continue

            # 両方存在する場合 → 重複削除
            if correct_port and output_2_port:
                print(f"🗑️  Run {process.run_id}, Process '{process.name}' (ID: {process.id}, Type: {process.process_type}):")
                print(f"    - {correct_port_name} (ID: {correct_port.id}): {correct_port.data_type}")
                print(f"    - output_2 (ID: {output_2_port.id}): {output_2_port.data_type}")

                # output_2を使用する接続を移行
                connections_using_output_2 = session.query(PortConnection).filter(
                    PortConnection.source_port_id == output_2_port.id
                ).all()

                if connections_using_output_2:
                    print(f"    → {len(connections_using_output_2)}個の接続を{correct_port_name}に移行")
                    for conn in connections_using_output_2:
                        conn.source_port_id = correct_port.id
                        migrated_count += 1

                # output_2ポートを削除
                print(f"    → output_2ポート (ID: {output_2_port.id}) を削除\n")
                session.delete(output_2_port)
                deleted_count += 1

        # コミット
        session.commit()

        print("=" * 60)
        print(f"✅ クリーンアップ完了:")
        print(f"   - 移行した接続: {migrated_count}個")
        print(f"   - 削除したポート: {deleted_count}個")
        print("=" * 60)

        # 検証
        print("\n=== 検証: 残りのoutput_2ポート ===")
        remaining = session.query(Port).filter(
            Port.port_name == "output_2",
            Port.port_type == "output"
        ).count()

        if remaining > 0:
            print(f"⚠️  警告: まだ{remaining}個のoutput_2ポートが残っています")
            # 詳細を表示
            remaining_ports = session.query(Port).join(Process).filter(
                Port.port_name == "output_2",
                Port.port_type == "output"
            ).all()
            for port in remaining_ports:
                process = session.query(Process).filter(Process.id == port.process_id).first()
                print(f"    - Run {process.run_id}, Process '{process.name}', Type: {process.process_type}")
        else:
            print("✅ すべてのoutput_2ポートが処理されました")

    except Exception as e:
        session.rollback()
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    cleanup_all_duplicate_ports()
