#!/usr/bin/env python3
"""
output_N形式の重複ポートクリーンアップスクリプト

問題:
- フォールバック生成により、output_2, output_3, output_4...という誤ったポート名が作成された
- YAMLで定義された正しいポート名（out1, value等）が既に存在する

解決策:
1. output_N形式のポートを検出
2. 正しいYAML定義のポートが存在するか確認
3. 接続を正しいポートに移行して重複ポートを削除
"""

import sys
from pathlib import Path
import re

sys.path.append("/app")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from define_db.models import Port, PortConnection, Process
from services.port_type_mapper import get_port_type_mapper

DB_PATH = "/data/sql_app.db"
engine = create_engine(f"sqlite:///{DB_PATH}")
SessionLocal = sessionmaker(bind=engine)


def cleanup_output_n_ports():
    """output_N形式の重複ポートをクリーンアップ"""
    session = SessionLocal()

    try:
        # ポート型マッパーを初期化
        type_mapper = get_port_type_mapper()

        print("=== output_N形式の重複ポートを検出 ===\n")

        # output_N形式のポート（output_2, output_3, output_4...）を検出
        all_ports = session.query(Port).filter(
            Port.port_type == "output"
        ).all()

        output_n_pattern = re.compile(r'^output_\d+$')
        output_n_ports = [p for p in all_ports if output_n_pattern.match(p.port_name)]

        print(f"検出したoutput_N形式のポート: {len(output_n_ports)}個\n")

        migrated_count = 0
        deleted_count = 0
        renamed_count = 0

        # プロセスごとにグループ化
        processes_with_output_n = {}
        for port in output_n_ports:
            if port.process_id not in processes_with_output_n:
                processes_with_output_n[port.process_id] = []
            processes_with_output_n[port.process_id].append(port)

        for process_id, ports in processes_with_output_n.items():
            process = session.query(Process).filter(Process.id == process_id).first()
            if not process:
                continue

            print(f"Run {process.run_id}, Process '{process.name}' (ID: {process.id}, Type: {process.process_type}):")

            # YAMLから正しいポート定義を取得
            correct_ports_def = []
            if process.process_type:
                all_ports_def = type_mapper.get_all_ports_for_process(process.process_type)
                if all_ports_def and all_ports_def.get('output'):
                    correct_ports_def = all_ports_def['output']

            if not correct_ports_def:
                print(f"  ⚠️  正しいポート定義が取得できません。スキップします。\n")
                continue

            # 各output_Nポートを処理
            for output_n_port in sorted(ports, key=lambda p: p.port_name):
                # output_Nの番号を取得（output_2 → 2）
                match = re.match(r'^output_(\d+)$', output_n_port.port_name)
                if not match:
                    continue

                n = int(match.group(1))
                # output_2は2番目の出力 → インデックス1
                # output_3は3番目の出力 → インデックス2
                port_index = n - 1

                if port_index >= len(correct_ports_def):
                    print(f"  ⚠️  {output_n_port.port_name}: ポート定義が存在しません（定義数: {len(correct_ports_def)}）")
                    # 定義外のポートは削除
                    connections = session.query(PortConnection).filter(
                        (PortConnection.source_port_id == output_n_port.id) |
                        (PortConnection.target_port_id == output_n_port.id)
                    ).count()

                    if connections > 0:
                        print(f"     → {connections}個の接続が存在するため、削除をスキップ\n")
                        continue
                    else:
                        print(f"     → 接続なし、ポートを削除\n")
                        session.delete(output_n_port)
                        deleted_count += 1
                    continue

                # 正しいポート名とデータ型を取得
                correct_port_def = correct_ports_def[port_index]
                correct_port_name = correct_port_def.get('id')
                correct_data_type = correct_port_def.get('type', 'Unknown')

                print(f"  - {output_n_port.port_name} (ID: {output_n_port.id}): {output_n_port.data_type}")
                print(f"    → 正しいポート名: {correct_port_name}, 型: {correct_data_type}")

                # 正しいポート名のポートが既に存在するか確認
                correct_port = session.query(Port).filter(
                    Port.process_id == process.id,
                    Port.port_name == correct_port_name,
                    Port.port_type == "output"
                ).first()

                if correct_port:
                    # 重複削除
                    print(f"    → 正しいポート (ID: {correct_port.id}) が既に存在")

                    # 接続を移行
                    connections_using_output_n = session.query(PortConnection).filter(
                        PortConnection.source_port_id == output_n_port.id
                    ).all()

                    if connections_using_output_n:
                        print(f"    → {len(connections_using_output_n)}個の接続を{correct_port_name}に移行")
                        for conn in connections_using_output_n:
                            conn.source_port_id = correct_port.id
                            migrated_count += 1

                    # output_Nポートを削除
                    print(f"    → {output_n_port.port_name}ポートを削除\n")
                    session.delete(output_n_port)
                    deleted_count += 1
                else:
                    # リネーム
                    print(f"    → {output_n_port.port_name} を {correct_port_name} にリネーム")
                    output_n_port.port_name = correct_port_name
                    output_n_port.data_type = correct_data_type
                    renamed_count += 1
                    print(f"    → データ型も更新: {correct_data_type}\n")

        # コミット
        session.commit()

        print("=" * 60)
        print(f"✅ クリーンアップ完了:")
        print(f"   - 移行した接続: {migrated_count}個")
        print(f"   - 削除したポート: {deleted_count}個")
        print(f"   - リネームしたポート: {renamed_count}個")
        print("=" * 60)

        # 検証
        print("\n=== 検証: 残りのoutput_Nポート ===")
        remaining_ports = session.query(Port).filter(
            Port.port_type == "output"
        ).all()

        remaining_output_n = [p for p in remaining_ports if output_n_pattern.match(p.port_name)]

        if remaining_output_n:
            print(f"⚠️  警告: まだ{len(remaining_output_n)}個のoutput_Nポートが残っています")
            for port in remaining_output_n[:10]:  # 最大10個表示
                process = session.query(Process).filter(Process.id == port.process_id).first()
                print(f"    - Run {process.run_id}, Process '{process.name}', Port: {port.port_name}, Type: {process.process_type}")
        else:
            print("✅ すべてのoutput_Nポートが処理されました")

    except Exception as e:
        session.rollback()
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    cleanup_output_n_ports()
