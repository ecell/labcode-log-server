from define_db.models import Process, Run, Operation
from define_db.database import SessionLocal
from api.response_model import ProcessResponse, ProcessListResponse, ProcessResponseEnhanced, ProcessDetailResponse, PortResponse
from fastapi import APIRouter, Query
from fastapi import Form
from fastapi import HTTPException
from datetime import datetime
import yaml
from pathlib import Path
from typing import Optional, Dict, List

router = APIRouter()


def load_port_info_from_yaml(storage_address: str, process_name: str, process_type: str = None) -> Optional[Dict]:
    """
    YAMLファイルからポート情報を取得する

    Args:
        storage_address: ストレージアドレス (Runのstorage_address)
        process_name: プロセス名 (protocol.yamlのoperations.id)
        process_type: プロセスタイプ (protocol.yamlのoperations.type)

    Returns:
        dict: ポート情報 {"input": [...], "output": [...]}
              または None (ファイルが存在しない/エラー発生時)
    """
    try:
        # protocol.yamlとmanipulate.yamlのパスを構築
        protocol_path = Path(storage_address) / "protocol.yaml"
        manipulate_path = Path(storage_address) / "manipulate.yaml"

        if not protocol_path.exists() or not manipulate_path.exists():
            print(f"YAML files not found at {storage_address}")
            return None

        # YAMLファイルを読み込み
        with open(protocol_path, 'r', encoding='utf-8') as f:
            protocol_data = yaml.safe_load(f)

        with open(manipulate_path, 'r', encoding='utf-8') as f:
            manipulate_data = yaml.safe_load(f)

        # protocol.yamlからプロセスタイプを取得（未指定の場合）
        if not process_type:
            operations = protocol_data.get('operations', [])
            for op in operations:
                if op.get('id') == process_name:
                    process_type = op.get('type')
                    break

        if not process_type:
            print(f"Process type not found for process: {process_name}")
            return None

        # manipulate.yamlから該当プロセスタイプのポート定義を取得
        process_type_def = None
        for proc_def in manipulate_data:
            if proc_def.get('name') == process_type:
                process_type_def = proc_def
                break

        if not process_type_def:
            print(f"Process type definition not found: {process_type}")
            return None

        # protocol.yamlのconnectionsからこのプロセスの接続情報を取得
        connections = protocol_data.get('connections', [])

        # 入力ポート・出力ポート情報を構築
        input_ports = []
        output_ports = []

        # 入力ポート
        for port_def in process_type_def.get('input', []):
            port_info = {
                'id': port_def.get('id'),
                'name': port_def.get('id'),
                'data_type': port_def.get('type'),
                'connected_from': None
            }

            # connectionsから接続元を検索
            # protocol.yamlの connections は:
            # - input: [source_process_id, source_port_id]
            #   output: [target_process_id, target_port_id]
            # の形式で、input側が出力元、output側が入力先
            for conn in connections:
                conn_output = conn.get('output', [])
                if (len(conn_output) >= 2 and
                    conn_output[0] == process_name and
                    conn_output[1] == port_def.get('id')):
                    # このプロセスのこのポートへの入力
                    conn_input = conn.get('input', [])
                    if len(conn_input) >= 2:
                        port_info['connected_from'] = f"{conn_input[0]}.{conn_input[1]}"

            input_ports.append(port_info)

        # 出力ポート
        for port_def in process_type_def.get('output', []):
            port_info = {
                'id': port_def.get('id'),
                'name': port_def.get('id'),
                'data_type': port_def.get('type'),
                'connected_to': None
            }

            # connectionsから接続先を検索
            for conn in connections:
                conn_input = conn.get('input', [])
                if (len(conn_input) >= 2 and
                    conn_input[0] == process_name and
                    conn_input[1] == port_def.get('id')):
                    # このプロセスのこのポートからの出力
                    conn_output = conn.get('output', [])
                    if len(conn_output) >= 2:
                        port_info['connected_to'] = f"{conn_output[0]}.{conn_output[1]}"

            output_ports.append(port_info)

        return {
            'input': input_ports,
            'output': output_ports
        }

    except yaml.YAMLError as e:
        print(f"YAML parse error: {e}")
        return None
    except PermissionError as e:
        print(f"Permission error accessing YAML files: {e}")
        return None
    except Exception as e:
        print(f"Error loading ports from YAML: {e}")
        return None


@router.get("/processes", tags=["processes"], response_model=ProcessListResponse)
def list_processes(
    limit: int = Query(100, ge=1, le=1000, description="取得件数"),
    offset: int = Query(0, ge=0, description="オフセット")
):
    """
    プロセス一覧を取得する（ページネーション対応）

    Args:
        limit: 取得件数（デフォルト: 100、最大: 1000）
        offset: オフセット（デフォルト: 0）

    Returns:
        ProcessListResponse: 総数とプロセスリスト

    注意:
        ProcessモデルにはDBレベルでtype/status/created_at/updated_atフィールドが
        存在しないため、現時点ではデフォルト値を返します。
        将来的にはYAMLファイルから動的に読み込む予定。
    """
    with SessionLocal() as session:
        # 総数を取得
        total = session.query(Process).count()

        # プロセス一覧を取得（ページネーション）
        processes = session.query(Process)\
            .offset(offset)\
            .limit(limit)\
            .all()

        # ProcessResponseEnhancedに変換
        # 注: type, status, created_at, updated_atはDBに存在しないため、
        # 一時的にデフォルト値を設定
        items = []
        for p in processes:
            items.append(ProcessResponseEnhanced(
                id=p.id,
                run_id=p.run_id,
                name=p.name,
                type="unknown",  # TODO: YAMLから取得
                status="completed",  # TODO: YAMLから取得または推定
                created_at=datetime.now(),  # TODO: YAMLまたはRunから取得
                updated_at=datetime.now()   # TODO: YAMLまたはRunから取得
            ))

        return ProcessListResponse(
            total=total,
            items=items
        )


@router.post("/processes/", tags=["processes"], response_model=ProcessResponse)
def create(
        name: str = Form(),
        run_id: int = Form(),
        storage_address: str = Form()
):
    with SessionLocal() as session:
        # Check run existence
        run = session.query(Run).filter(Run.id == run_id).first()
        if not run:
            raise HTTPException(status_code=400, detail=f"Run with id {run_id} not found")
        process_to_add = Process(
            name=name,
            run_id=run_id,
            storage_address=storage_address
        )
        session.add_all([process_to_add])
        session.commit()
        session.refresh(process_to_add)
        return ProcessResponse.model_validate(process_to_add)


@router.get("/processes/{id}", tags=["processes"], response_model=ProcessDetailResponse)
def read(id: int):
    """
    プロセス詳細を取得する（ポート情報を含む）

    Args:
        id: プロセスID

    Returns:
        ProcessDetailResponse: プロセス詳細（ポート情報含む）
    """
    with SessionLocal() as session:
        # プロセス基本情報を取得
        process = session.query(Process).filter(Process.id == id).first()
        if not process:
            raise HTTPException(status_code=404, detail="Process not found")

        # Runを取得してstorage_addressとタイムスタンプを参照
        run = session.query(Run).filter(Run.id == process.run_id).first()
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")

        # YAMLファイルからポート情報を取得
        ports = None
        if run.storage_address:
            try:
                ports = load_port_info_from_yaml(
                    storage_address=run.storage_address,
                    process_name=process.name
                )
            except Exception as e:
                # YAMLパース失敗時はログに記録し、ポート情報なしで返却
                print(f"Failed to load port info for process {id}: {e}")
                ports = None

        # ProcessDetailResponseを構築
        return ProcessDetailResponse(
            id=process.id,
            run_id=process.run_id,
            name=process.name,
            type="unknown",  # TODO: YAMLから取得
            status="completed",  # TODO: Runのstatusから推定
            created_at=run.added_at if run.added_at else datetime.now(),
            updated_at=datetime.now(),
            ports=ports,
            storage_address=process.storage_address,
            started_at=run.started_at,
            finished_at=run.finished_at
        )


@router.put("/processes/{id}", tags=["processes"], response_model=ProcessResponse)
def update(
        id: int,
        name: str = Form(),
        run_id: int = Form(),
        storage_address: str = Form()
):
    with SessionLocal() as session:
        # Check process existence
        process = session.query(Process).filter(Process.id == id).first()
        if not process:
            raise HTTPException(status_code=404, detail="Process not found")
        # Check run existence
        run = session.query(Run).filter(Run.id == run_id).first()
        if not run:
            raise HTTPException(status_code=400, detail=f"Run with id {run_id} not found")
        process.name = name
        process.run_id = run_id
        process.storage_address = storage_address
        session.commit()
        session.refresh(process)
        return ProcessResponse.model_validate(process)


@router.patch("/processes/{id}", tags=["processes"], response_model=ProcessResponse)
def patch(id: int, attribute: str = Form(), new_value: str = Form()):
    with SessionLocal() as session:
        process = session.query(Process).filter(Process.id == id).first()
        if not process:
            raise HTTPException(status_code=404, detail="Process not found")
        match attribute:
            case "name":
                process.name = new_value
            case "run_id":
                # Check run existence
                run = session.query(Run).filter(Run.id == new_value).first()
                if not run:
                    raise HTTPException(status_code=400, detail=f"Run with id {new_value} not found")
                process.run_id = new_value
            case "storage_address":
                process.storage_address = new_value
            case _:
                raise HTTPException(status_code=400, detail="Invalid attribute")
        session.commit()
        session.refresh(process)
        return ProcessResponse.model_validate(process)


@router.delete("/processes/{id}", tags=["processes"])
def delete(id: int):
    with SessionLocal() as session:
        process = session.query(Process).filter(Process.id == id).first()
        if not process:
            raise HTTPException(status_code=404, detail="Process not found")
        session.delete(process)
        session.commit()
        return {"message": "Process deleted successfully"}


@router.get("/processes/{id}/operations", tags=["processes"])
def get_operations_by_process(id: int):
    """
    指定されたProcess IDのOperation一覧を取得

    Args:
        id: プロセスID

    Returns:
        List[dict]: オペレーション一覧
    """
    with SessionLocal() as session:
        # Processの存在確認
        process = session.query(Process).filter(Process.id == id).first()
        if not process:
            raise HTTPException(status_code=404, detail=f"Process with id {id} not found")

        # Operation一覧を取得
        operations = session.query(Operation).filter(Operation.process_id == id).all()

        return [
            {
                "id": op.id,
                "process_id": op.process_id,
                "name": op.name,
                "type": op.type if hasattr(op, 'type') else None,
                "status": op.status if hasattr(op, 'status') else None
            }
            for op in operations
        ]
