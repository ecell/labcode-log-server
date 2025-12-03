"""
Portsテーブル関連のテストコード
"""
import pytest
from sqlalchemy.orm import Session
from define_db.models import User, Project, Run, Process, Port, PortConnection
from define_db.database import SessionLocal, engine, Base
from datetime import datetime


@pytest.fixture(scope="function")
def test_db():
    """テスト用DB作成"""
    Base.metadata.create_all(bind=engine)
    session = SessionLocal()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)


def test_port_creation(test_db: Session):
    """Port作成テスト"""
    # 前提データ作成
    user = User(id=1, email="test@example.com")
    project = Project(id=1, name="Test Project", user_id=1, created_at=datetime.now(), updated_at=datetime.now())
    run = Run(id=1, project_id=1, file_name="test.yaml", checksum="abc", user_id=1,
              added_at=datetime.now(), status="running", storage_address="/data/runs/1")
    process = Process(id=1, name="serve_plate1", run_id=1, storage_address="/data/processes/1")

    test_db.add_all([user, project, run, process])
    test_db.commit()

    # Port作成
    port = Port(
        process_id=1,
        port_name="value",
        port_type="output",
        data_type="Plate96",
        position=0,
        is_required=True
    )
    test_db.add(port)
    test_db.commit()

    # 確認
    retrieved_port = test_db.query(Port).filter(Port.id == port.id).first()
    assert retrieved_port is not None
    assert retrieved_port.port_name == "value"
    assert retrieved_port.data_type == "Plate96"


def test_port_connection_creation(test_db: Session):
    """PortConnection作成テスト"""
    # 前提データ作成
    user = User(id=1, email="test@example.com")
    project = Project(id=1, name="Test Project", user_id=1, created_at=datetime.now(), updated_at=datetime.now())
    run = Run(id=1, project_id=1, file_name="test.yaml", checksum="abc", user_id=1,
              added_at=datetime.now(), status="running", storage_address="/data/runs/1")
    process1 = Process(id=1, name="serve_plate1", run_id=1, storage_address="/data/processes/1")
    process2 = Process(id=2, name="dispense_liquid1", run_id=1, storage_address="/data/processes/2")
    port1 = Port(id=1, process_id=1, port_name="value", port_type="output", data_type="Plate96", position=0, is_required=True)
    port2 = Port(id=2, process_id=2, port_name="in1", port_type="input", data_type="Plate96", position=0, is_required=True)

    test_db.add_all([user, project, run, process1, process2, port1, port2])
    test_db.commit()

    # Connection作成
    connection = PortConnection(
        run_id=1,
        source_port_id=1,
        target_port_id=2
    )
    test_db.add(connection)
    test_db.commit()

    # 確認
    retrieved = test_db.query(PortConnection).filter(PortConnection.id == connection.id).first()
    assert retrieved is not None
    assert retrieved.source_port_id == 1
    assert retrieved.target_port_id == 2


def test_cascade_delete_process_to_ports(test_db: Session):
    """CASCADE DELETE: Process削除→Port削除"""
    # 前提データ作成
    user = User(id=1, email="test@example.com")
    project = Project(id=1, name="Test Project", user_id=1, created_at=datetime.now(), updated_at=datetime.now())
    run = Run(id=1, project_id=1, file_name="test.yaml", checksum="abc", user_id=1,
              added_at=datetime.now(), status="running", storage_address="/data/runs/1")
    process = Process(id=1, name="serve_plate1", run_id=1, storage_address="/data/processes/1")
    port = Port(id=1, process_id=1, port_name="value", port_type="output", data_type="Plate96", position=0, is_required=True)

    test_db.add_all([user, project, run, process, port])
    test_db.commit()

    # Process削除
    test_db.delete(process)
    test_db.commit()

    # Port削除確認
    retrieved_port = test_db.query(Port).filter(Port.id == 1).first()
    assert retrieved_port is None
