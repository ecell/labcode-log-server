#!/usr/bin/env python3
"""
重複ポートクリーンアップスクリプト v2
コンテナ内で実行するバージョン
"""

import sys
from pathlib import Path

# Dockerコンテナ内のパス
sys.path.append("/app")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from define_db.models import Port, PortConnection, Process

# コンテナ内のデータベースパス
DB_PATH = "/data/sql_app.db"
engine = create_engine(f"sqlite:///{DB_PATH}")
SessionLocal = sessionmaker(bind=engine)


def cleanup_duplicate_ports():
    """重複ポートをクリーンアップ"""
    session = SessionLocal()

    try:
        # ステップ1: 重複ポートを持つプロセスを特定
        print("=== ステップ1: 重複ポートを持つプロセスを検出 ===")

        # DispenseLiquid96Wellsのプロセスを取得
        processes = session.query(Process).filter(
            Process.process_type == "DispenseLiquid96Wells"
        ).all()

        migrated_count = 0
        deleted_count = 0

        for process in processes:
            # out1とoutput_2の両方を持つか確認
            out1_port = session.query(Port).filter(
                Port.process_id == process.id,
                Port.port_name == "out1",
                Port.port_type == "output"
            ).first()

            output_2_port = session.query(Port).filter(
                Port.process_id == process.id,
                Port.port_name == "output_2",
                Port.port_type == "output"
            ).first()

            if not (out1_port and output_2_port):
                continue

            print(f"\nRun {process.run_id}, Process '{process.name}' (ID: {process.id}):")
            print(f"  - out1 (ID: {out1_port.id}): {out1_port.data_type}")
            print(f"  - output_2 (ID: {output_2_port.id}): {output_2_port.data_type}")

            # ステップ2: output_2を使用する接続を取得
            connections_using_output_2 = session.query(PortConnection).filter(
                PortConnection.source_port_id == output_2_port.id
            ).all()

            if connections_using_output_2:
                print(f"  → {len(connections_using_output_2)}個の接続がoutput_2を使用")

                # ステップ3: 接続をout1に移行
                for conn in connections_using_output_2:
                    print(f"    接続 {conn.id}: source_port_id {conn.source_port_id} → {out1_port.id}")
                    conn.source_port_id = out1_port.id
                    migrated_count += 1

            # ステップ4: output_2ポートを削除
            print(f"  → output_2ポート (ID: {output_2_port.id}) を削除")
            session.delete(output_2_port)
            deleted_count += 1

        # コミット
        session.commit()

        print("\n" + "=" * 60)
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
        else:
            print("✅ すべてのoutput_2ポートが削除されました")

    except Exception as e:
        session.rollback()
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    cleanup_duplicate_ports()
