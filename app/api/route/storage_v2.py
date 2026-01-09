"""Storage API v2

Hybrid Access Layer (HAL) を使用した新しいストレージAPI。
Run IDベースのアクセスで、S3/ローカル/DBデータを統一的に扱う。
"""

import logging
import os
import io
import json
import tempfile
import sqlite3
import zipfile
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, Body
from fastapi.responses import PlainTextResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from define_db.database import get_db
from define_db.models import Run, Process, Operation, Edge, Port
from services.hal import HybridAccessLayer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/storage", tags=["storage-v2"])


# ==================== Request/Response Models ====================

class BatchDownloadV2Request(BaseModel):
    """HAL対応バッチダウンロードリクエスト"""
    run_ids: List[int] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="ダウンロード対象のランIDリスト"
    )


class BatchDownloadV2Estimate(BaseModel):
    """HAL対応バッチダウンロード推定サイズレスポンス"""
    run_count: int
    total_files: int
    estimated_size_bytes: int
    estimated_size_mb: float
    can_download: bool
    message: Optional[str] = None
    runs_detail: List[dict] = []


@router.get("/list/{run_id}")
def list_run_contents(
    run_id: int,
    prefix: str = Query("", description="仮想パスプレフィックス"),
    db: Session = Depends(get_db)
):
    """
    Run内のコンテンツ一覧を取得

    S3モード: S3ファイル一覧
    ローカルモード: DBデータを仮想ファイルとして表示
    """
    try:
        hal = HybridAccessLayer(db)
        items = hal.list_contents(run_id, prefix)
        return {
            "run_id": run_id,
            "prefix": prefix,
            "items": [item.to_dict() for item in items]
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        logger.error(f"Runtime error in list_run_contents: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in list_run_contents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/content/{run_id}")
def load_content(
    run_id: int,
    path: str = Query(..., description="仮想パス"),
    db: Session = Depends(get_db)
):
    """
    コンテンツを取得（プレビュー用）

    テキストファイルの場合は文字列として返却
    """
    try:
        hal = HybridAccessLayer(db)
        content = hal.load_content(run_id, path)

        if content is None:
            raise HTTPException(status_code=404, detail="Content not found")

        # テキストとして返却
        try:
            text = content.decode('utf-8')
            return {"content": text, "encoding": "utf-8"}
        except UnicodeDecodeError:
            # バイナリの場合はBase64エンコード
            import base64
            return {"content": base64.b64encode(content).decode(), "encoding": "base64"}

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in load_content: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/download/{run_id}")
def get_download_url(
    run_id: int,
    path: str = Query(..., description="仮想パス"),
    db: Session = Depends(get_db)
):
    """ダウンロードURLを取得"""
    try:
        hal = HybridAccessLayer(db)
        url = hal.get_download_url(run_id, path)
        return {"url": url, "run_id": run_id, "path": path}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_download_url: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/info/{run_id}")
def get_storage_info(
    run_id: int,
    db: Session = Depends(get_db)
):
    """Runのストレージ情報を取得"""
    try:
        hal = HybridAccessLayer(db)
        info = hal.get_storage_info(run_id)
        return info.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_storage_info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/db-content/{run_id}")
def get_db_content(
    run_id: int,
    path: str = Query(..., description="仮想パス"),
    op_id: Optional[int] = Query(None, description="Operation ID"),
    db: Session = Depends(get_db)
):
    """DBに保存されたコンテンツを直接取得"""
    # オペレーションログの取得
    if "operations/" in path and path.endswith("log.txt") and op_id:
        operation = db.query(Operation).filter(Operation.id == op_id).first()
        if operation and operation.log:
            return PlainTextResponse(
                content=operation.log,
                media_type="text/plain",
                headers={"Content-Disposition": f"attachment; filename=log_{op_id}.txt"}
            )

    raise HTTPException(status_code=404, detail="Content not found in database")


@router.get("/dump/{run_id}")
def download_sql_dump(
    run_id: int,
    db: Session = Depends(get_db)
):
    """
    Run関連データのSQLiteダンプをダウンロード

    全ストレージモード（S3/local/hybrid）に対応。
    Run関連の全メタデータを独立したSQLiteファイルとしてエクスポートする。

    含まれるデータ:
    - runs: 該当Run
    - processes: Run内のProcess
    - operations: Process内のOperation（S3モードではlogフィールドは空の場合あり）
    - edges: Run内のEdge
    - ports: Process内のPort
    """
    # Runの存在確認
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    try:
        # 一時ファイルを作成
        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            suffix='.db',
            prefix=f'run_{run_id}_'
        )
        temp_path = temp_file.name
        temp_file.close()

        # 新しいSQLiteデータベースを作成
        conn = sqlite3.connect(temp_path)
        cursor = conn.cursor()

        # テーブル作成
        cursor.execute('''
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                file_name TEXT,
                checksum TEXT,
                user_id INTEGER,
                added_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                storage_address TEXT,
                storage_mode TEXT,
                deleted_at TEXT,
                display_visible INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE processes (
                id INTEGER PRIMARY KEY,
                name TEXT,
                run_id INTEGER,
                storage_address TEXT,
                process_type TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE operations (
                id INTEGER PRIMARY KEY,
                process_id INTEGER,
                name TEXT,
                parent_id INTEGER,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                storage_address TEXT,
                is_transport INTEGER,
                is_data INTEGER,
                log TEXT,
                FOREIGN KEY (process_id) REFERENCES processes(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE edges (
                id INTEGER PRIMARY KEY,
                run_id INTEGER,
                from_id INTEGER,
                to_id INTEGER,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE ports (
                id INTEGER PRIMARY KEY,
                process_id INTEGER,
                port_name TEXT,
                port_type TEXT,
                data_type TEXT,
                position INTEGER,
                is_required INTEGER,
                default_value TEXT,
                description TEXT,
                FOREIGN KEY (process_id) REFERENCES processes(id)
            )
        ''')

        # Runデータを挿入
        cursor.execute('''
            INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            run.id, run.project_id, run.file_name, run.checksum, run.user_id,
            run.added_at.isoformat() if run.added_at else None,
            run.started_at.isoformat() if run.started_at else None,
            run.finished_at.isoformat() if run.finished_at else None,
            run.status, run.storage_address, run.storage_mode,
            run.deleted_at.isoformat() if run.deleted_at else None,
            1 if run.display_visible else 0
        ))

        # Processデータを取得・挿入
        processes = db.query(Process).filter(Process.run_id == run_id).all()
        process_ids = [p.id for p in processes]

        for p in processes:
            cursor.execute('''
                INSERT INTO processes VALUES (?, ?, ?, ?, ?)
            ''', (p.id, p.name, p.run_id, p.storage_address, p.process_type))

        # Operationデータを挿入
        if process_ids:
            operations = db.query(Operation).filter(
                Operation.process_id.in_(process_ids)
            ).all()

            for op in operations:
                cursor.execute('''
                    INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    op.id, op.process_id, op.name, op.parent_id,
                    op.started_at.isoformat() if op.started_at else None,
                    op.finished_at.isoformat() if op.finished_at else None,
                    op.status, op.storage_address,
                    1 if op.is_transport else 0,
                    1 if op.is_data else 0,
                    op.log
                ))

            # Portデータを挿入
            ports = db.query(Port).filter(
                Port.process_id.in_(process_ids)
            ).all()

            for port in ports:
                cursor.execute('''
                    INSERT INTO ports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    port.id, port.process_id, port.port_name, port.port_type,
                    port.data_type, port.position,
                    1 if port.is_required else 0,
                    getattr(port, 'default_value', None),
                    getattr(port, 'description', None)
                ))

        # Edgeデータを挿入
        edges = db.query(Edge).filter(Edge.run_id == run_id).all()
        for e in edges:
            cursor.execute('''
                INSERT INTO edges VALUES (?, ?, ?, ?)
            ''', (e.id, e.run_id, e.from_id, e.to_id))

        conn.commit()
        conn.close()

        # ファイルサイズをログ
        file_size = os.path.getsize(temp_path)
        logger.info(f"Created SQL dump for run {run_id}: {file_size} bytes")

        # FileResponseで返却（cleanup後に自動削除）
        return FileResponse(
            path=temp_path,
            filename=f"run_{run_id}_dump.db",
            media_type="application/x-sqlite3",
            background=None  # 同期的に処理
        )

    except Exception as e:
        logger.error(f"Error creating SQL dump for run {run_id}: {e}")
        # 一時ファイルがあれば削除
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        raise HTTPException(status_code=500, detail=f"Failed to create SQL dump: {str(e)}")


# ==================== HAL-based Batch Download ====================

MAX_BATCH_SIZE = 500 * 1024 * 1024  # 500MB


def _collect_all_files_recursive(hal: HybridAccessLayer, run_id: int, prefix: str = "") -> List[dict]:
    """
    HALを使用してRun内の全ファイルを再帰的に収集

    Args:
        hal: HybridAccessLayer instance
        run_id: Run ID
        prefix: 検索プレフィックス

    Returns:
        ファイル情報のリスト
    """
    files = []
    items = hal.list_contents(run_id, prefix)

    for item in items:
        if item.type == "file":
            files.append({
                "path": item.path,
                "size": item.size or 0,
                "source": item.source.value if item.source else "unknown"
            })
        elif item.type == "directory":
            # ディレクトリは再帰的に探索
            sub_prefix = item.path if item.path.endswith('/') else item.path + '/'
            files.extend(_collect_all_files_recursive(hal, run_id, sub_prefix))

    return files


@router.post("/batch-download")
def batch_download_v2(
    request: BatchDownloadV2Request = Body(...),
    db: Session = Depends(get_db)
):
    """
    HALを使用した複数ランの一括ダウンロード（Storage Browser相当のフォルダ構成）

    全ストレージモード（S3/local/hybrid/unknown）に対応。
    HALを通じてStorage Browserと同じフォルダ構成でZIPを生成。

    Args:
        request: BatchDownloadV2Request（run_ids）
        db: Database session

    Returns:
        StreamingResponse: ZIPファイル
    """
    if not request.run_ids:
        raise HTTPException(status_code=400, detail="run_ids is required")

    hal = HybridAccessLayer(db)

    # ランを取得
    runs = db.query(Run).filter(Run.id.in_(request.run_ids)).all()
    if not runs:
        raise HTTPException(status_code=404, detail="No runs found")

    # ZIPをメモリ上で作成
    zip_buffer = io.BytesIO()
    manifest_data = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "runs": [],
        "total_files": 0,
        "total_size": 0,
        "errors": []
    }

    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for run in runs:
                run_prefix = f"run_{run.id}/"
                run_file_count = 0
                run_size = 0

                try:
                    # HALを使用して全ファイルを再帰収集
                    files = _collect_all_files_recursive(hal, run.id)

                    for file_info in files:
                        file_path = file_info["path"]
                        file_size = file_info["size"]

                        # コンテンツを読み込み
                        content = hal.load_content(run.id, file_path)
                        if content:
                            # ZIPに追加（run_{id}/相対パス の構造）
                            zip_path = run_prefix + file_path
                            zf.writestr(zip_path, content)

                            run_file_count += 1
                            run_size += len(content)

                    # メタデータダンプも追加（オプション）
                    try:
                        dump_content = _generate_metadata_dump(db, run.id)
                        if dump_content:
                            zf.writestr(f"{run_prefix}_metadata.db", dump_content)
                    except Exception as e:
                        logger.warning(f"Failed to generate metadata dump for run {run.id}: {e}")

                    manifest_data["runs"].append({
                        "run_id": run.id,
                        "storage_mode": run.storage_mode,
                        "file_count": run_file_count,
                        "total_size": run_size
                    })
                    manifest_data["total_files"] += run_file_count
                    manifest_data["total_size"] += run_size

                except Exception as e:
                    logger.error(f"Error processing run {run.id}: {e}")
                    manifest_data["errors"].append({
                        "run_id": run.id,
                        "error": str(e)
                    })

            # マニフェストを追加
            zf.writestr("manifest.json", json.dumps(manifest_data, indent=2, ensure_ascii=False))

        # バッファを先頭に戻す
        zip_buffer.seek(0)

        # ファイル名を生成
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"labcode_runs_{timestamp}.zip"

        return StreamingResponse(
            iter([zip_buffer.getvalue()]),
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
        )

    except Exception as e:
        logger.error(f"Error creating batch download ZIP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create ZIP: {str(e)}")


def _generate_metadata_dump(db: Session, run_id: int) -> Optional[bytes]:
    """
    メタデータダンプをバイト列として生成（SQLite形式）

    Args:
        db: Database session
        run_id: Run ID

    Returns:
        SQLiteデータベースのバイト列
    """
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        return None

    # 一時ファイルに作成
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_path = temp_file.name
    temp_file.close()

    try:
        conn = sqlite3.connect(temp_path)
        cursor = conn.cursor()

        # テーブル作成
        cursor.execute('''
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                file_name TEXT,
                checksum TEXT,
                user_id INTEGER,
                added_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                storage_address TEXT,
                storage_mode TEXT,
                deleted_at TEXT,
                display_visible INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE processes (
                id INTEGER PRIMARY KEY,
                name TEXT,
                run_id INTEGER,
                storage_address TEXT,
                process_type TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE operations (
                id INTEGER PRIMARY KEY,
                process_id INTEGER,
                name TEXT,
                parent_id INTEGER,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                storage_address TEXT,
                is_transport INTEGER,
                is_data INTEGER,
                log TEXT,
                FOREIGN KEY (process_id) REFERENCES processes(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE edges (
                id INTEGER PRIMARY KEY,
                run_id INTEGER,
                from_id INTEGER,
                to_id INTEGER,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE ports (
                id INTEGER PRIMARY KEY,
                process_id INTEGER,
                port_name TEXT,
                port_type TEXT,
                data_type TEXT,
                position INTEGER,
                is_required INTEGER,
                default_value TEXT,
                description TEXT,
                FOREIGN KEY (process_id) REFERENCES processes(id)
            )
        ''')

        # データを挿入
        cursor.execute('''
            INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            run.id, run.project_id, run.file_name, run.checksum, run.user_id,
            run.added_at.isoformat() if run.added_at else None,
            run.started_at.isoformat() if run.started_at else None,
            run.finished_at.isoformat() if run.finished_at else None,
            run.status, run.storage_address, run.storage_mode,
            run.deleted_at.isoformat() if run.deleted_at else None,
            1 if run.display_visible else 0
        ))

        processes = db.query(Process).filter(Process.run_id == run_id).all()
        process_ids = [p.id for p in processes]

        for p in processes:
            cursor.execute('''
                INSERT INTO processes VALUES (?, ?, ?, ?, ?)
            ''', (p.id, p.name, p.run_id, p.storage_address, p.process_type))

        if process_ids:
            operations = db.query(Operation).filter(
                Operation.process_id.in_(process_ids)
            ).all()

            for op in operations:
                cursor.execute('''
                    INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    op.id, op.process_id, op.name, op.parent_id,
                    op.started_at.isoformat() if op.started_at else None,
                    op.finished_at.isoformat() if op.finished_at else None,
                    op.status, op.storage_address,
                    1 if op.is_transport else 0,
                    1 if op.is_data else 0,
                    op.log
                ))

            ports = db.query(Port).filter(
                Port.process_id.in_(process_ids)
            ).all()

            for port in ports:
                cursor.execute('''
                    INSERT INTO ports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    port.id, port.process_id, port.port_name, port.port_type,
                    port.data_type, port.position,
                    1 if port.is_required else 0,
                    getattr(port, 'default_value', None),
                    getattr(port, 'description', None)
                ))

        edges = db.query(Edge).filter(Edge.run_id == run_id).all()
        for e in edges:
            cursor.execute('''
                INSERT INTO edges VALUES (?, ?, ?, ?)
            ''', (e.id, e.run_id, e.from_id, e.to_id))

        conn.commit()
        conn.close()

        # ファイルを読み込み
        with open(temp_path, 'rb') as f:
            content = f.read()

        return content

    finally:
        # 一時ファイルを削除
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@router.post("/batch-dump")
def batch_dump_metadata(
    request: BatchDownloadV2Request = Body(...),
    db: Session = Depends(get_db)
):
    """
    複数ランのメタデータダンプを一括ダウンロード

    各ランごとに個別の.dbファイルを生成し、ZIPにまとめて返却。
    ファイル実体なしでメタデータのみダウンロードしたい場合に使用。

    Args:
        request: BatchDownloadV2Request（run_ids）
        db: Database session

    Returns:
        StreamingResponse: ZIPファイル
    """
    if not request.run_ids:
        raise HTTPException(status_code=400, detail="run_ids is required")

    runs = db.query(Run).filter(Run.id.in_(request.run_ids)).all()
    if not runs:
        raise HTTPException(status_code=404, detail="No runs found")

    # ZIPをメモリ上で作成
    zip_buffer = io.BytesIO()
    manifest_data = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "type": "metadata_dumps",
        "runs": [],
        "errors": []
    }

    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for run in runs:
                try:
                    dump_content = _generate_metadata_dump(db, run.id)
                    if dump_content:
                        zf.writestr(f"run_{run.id}_dump.db", dump_content)
                        manifest_data["runs"].append({
                            "run_id": run.id,
                            "storage_mode": run.storage_mode,
                            "dump_size": len(dump_content)
                        })
                except Exception as e:
                    logger.error(f"Error generating dump for run {run.id}: {e}")
                    manifest_data["errors"].append({
                        "run_id": run.id,
                        "error": str(e)
                    })

            # マニフェストを追加
            zf.writestr("manifest.json", json.dumps(manifest_data, indent=2, ensure_ascii=False))

        zip_buffer.seek(0)

        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"labcode_metadata_dumps_{timestamp}.zip"

        return StreamingResponse(
            iter([zip_buffer.getvalue()]),
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
        )

    except Exception as e:
        logger.error(f"Error creating batch dump ZIP: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create ZIP: {str(e)}")


@router.post("/batch-download/estimate", response_model=BatchDownloadV2Estimate)
def estimate_batch_download_v2(
    request: BatchDownloadV2Request = Body(...),
    db: Session = Depends(get_db)
):
    """
    HAL対応バッチダウンロードの推定サイズを取得

    Args:
        request: BatchDownloadV2Request（run_ids）
        db: Database session

    Returns:
        BatchDownloadV2Estimate
    """
    if not request.run_ids:
        return BatchDownloadV2Estimate(
            run_count=0,
            total_files=0,
            estimated_size_bytes=0,
            estimated_size_mb=0.0,
            can_download=False,
            message="run_ids is required"
        )

    hal = HybridAccessLayer(db)
    runs = db.query(Run).filter(Run.id.in_(request.run_ids)).all()

    if not runs:
        return BatchDownloadV2Estimate(
            run_count=0,
            total_files=0,
            estimated_size_bytes=0,
            estimated_size_mb=0.0,
            can_download=False,
            message="No runs found"
        )

    total_size = 0
    total_files = 0
    runs_detail = []

    for run in runs:
        try:
            files = _collect_all_files_recursive(hal, run.id)
            run_size = sum(f["size"] for f in files)
            run_files = len(files)

            total_size += run_size
            total_files += run_files

            runs_detail.append({
                "run_id": run.id,
                "storage_mode": run.storage_mode,
                "file_count": run_files,
                "estimated_size": run_size
            })
        except Exception as e:
            logger.warning(f"Error estimating run {run.id}: {e}")
            runs_detail.append({
                "run_id": run.id,
                "error": str(e)
            })

    can_download = total_size <= MAX_BATCH_SIZE
    message = None if can_download else f"Estimated size ({total_size // (1024*1024)}MB) exceeds limit (500MB)"

    return BatchDownloadV2Estimate(
        run_count=len(runs),
        total_files=total_files,
        estimated_size_bytes=total_size,
        estimated_size_mb=round(total_size / (1024 * 1024), 2),
        can_download=can_download,
        message=message,
        runs_detail=runs_detail
    )
