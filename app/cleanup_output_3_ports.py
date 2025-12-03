#!/usr/bin/env python3
"""
output_3ポートのクリーンアップスクリプト

問題:
- ReadAbsorbance3Colorsに3番目の出力ポートoutput_3が誤って作成された
- 正しくは2つの出力（out1, value）のみ
- output_3の接続をvalueポートに移行して削除

戦略:
1. output_3を使用する接続を検出
2. 同じプロセスのvalueポートに接続を移行
3. output_3ポートを削除
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


def cleanup_output_3_ports():
    """output_3ポートをクリーンアップ"""
    session = SessionLocal()

    try:
        print("=== output_3ポートのクリーンアップ ===\n")

        # output_3ポートを持つプロセスを取得
        output_3_ports = session.query(Port).filter(
            Port.port_name == "output_3",
            Port.port_type == "output"
        ).all()

        print(f"検出したoutput_3ポート: {len(output_3_ports)}個\n")

        migrated_count = 0
        deleted_count = 0
        skipped_count = 0

        for output_3_port in output_3_ports:
            process = session.query(Process).filter(Process.id == output_3_port.process_id).first()
            if not process:
                continue

            print(f"Run {process.run_id}, Process '{process.name}' (ID: {process.id}, Type: {process.process_type}):")
            print(f"  - output_3 (ID: {output_3_port.id}): {output_3_port.data_type}")

            # 同じプロセスのvalueポートを取得
            value_port = session.query(Port).filter(
                Port.process_id == process.id,
                Port.port_name == "value",
                Port.port_type == "output"
            ).first()

            if not value_port:
                print(f"  ⚠️  valueポートが見つかりません。スキップします。\n")
                skipped_count += 1
                continue

            print(f"  - value (ID: {value_port.id}): {value_port.data_type}")

            # output_3を使用する接続を取得
            connections_using_output_3 = session.query(PortConnection).filter(
                PortConnection.source_port_id == output_3_port.id
            ).all()

            if connections_using_output_3:
                print(f"  → {len(connections_using_output_3)}個の接続をvalueポートに移行")
                for conn in connections_using_output_3:
                    # ターゲットポート情報を表示
                    target_port = session.query(Port).filter(Port.id == conn.target_port_id).first()
                    target_process = session.query(Process).filter(Process.id == target_port.process_id).first() if target_port else None

                    if target_process and target_port:
                        print(f"    接続 {conn.id}: output_3 → {target_process.name}.{target_port.port_name}")
                        print(f"                   変更後: value → {target_process.name}.{target_port.port_name}")

                    # 接続をvalueポートに移行
                    conn.source_port_id = value_port.id
                    migrated_count += 1
            else:
                print(f"  → 接続なし")

            # output_3ポートを削除
            print(f"  → output_3ポート (ID: {output_3_port.id}) を削除\n")
            session.delete(output_3_port)
            deleted_count += 1

        # コミット
        session.commit()

        print("=" * 60)
        print(f"✅ クリーンアップ完了:")
        print(f"   - 移行した接続: {migrated_count}個")
        print(f"   - 削除したポート: {deleted_count}個")
        print(f"   - スキップ: {skipped_count}個")
        print("=" * 60)

        # 検証
        print("\n=== 検証: 残りのoutput_3ポート ===")
        remaining = session.query(Port).filter(
            Port.port_name == "output_3",
            Port.port_type == "output"
        ).count()

        if remaining > 0:
            print(f"⚠️  警告: まだ{remaining}個のoutput_3ポートが残っています")
        else:
            print("✅ すべてのoutput_3ポートが削除されました")

    except Exception as e:
        session.rollback()
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    cleanup_output_3_ports()
