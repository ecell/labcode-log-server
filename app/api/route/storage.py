"""ストレージAPI（S3連携）

S3バケット内のファイル操作を行うAPIエンドポイント:
- GET /api/storage/list: ファイル一覧取得
- GET /api/storage/preview: ファイルプレビュー
- GET /api/storage/download: ダウンロードURL生成
- POST /api/storage/batch-download: 一括ダウンロード（ZIP形式）
"""

from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from botocore.exceptions import ClientError
from services.s3_service import S3Service, get_content_type
from services.zip_service import (
    ZipStreamService,
    SizeLimitExceededError,
    RunNotFoundError
)
from define_db.database import get_db
from define_db.models import Run
from services.storage_service import get_storage
import logging
import os

logger = logging.getLogger(__name__)

router = APIRouter()


# ==================== Response Models ====================

class StorageInfoResponse(BaseModel):
    """ストレージ情報レスポンス"""
    mode: str  # 's3' or 'local'
    bucket_name: Optional[str] = None  # S3バケット名（S3モードのみ）
    local_path: Optional[str] = None  # ローカルパス（ローカルモードのみ）
    db_path: Optional[str] = None  # SQLiteデータベースパス（ローカルモードのみ）

class FileItem(BaseModel):
    """ファイル情報"""
    name: str
    type: str  # 'file' or 'directory'
    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    extension: Optional[str] = None


class DirectoryItem(BaseModel):
    """ディレクトリ情報"""
    name: str
    type: str = 'directory'
    path: str


class PaginationInfo(BaseModel):
    """ページネーション情報"""
    total: int
    page: int
    per_page: int
    total_pages: int


class ListResponse(BaseModel):
    """ファイル一覧レスポンス"""
    files: List[FileItem]
    directories: List[DirectoryItem]
    pagination: PaginationInfo


class PreviewResponse(BaseModel):
    """プレビューレスポンス"""
    content: str
    content_type: str
    size: int
    last_modified: str
    truncated: bool


class DownloadResponse(BaseModel):
    """ダウンロードURLレスポンス"""
    download_url: str
    expires_at: str


class BatchDownloadRequest(BaseModel):
    """バッチダウンロードリクエスト"""
    run_ids: List[int] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="ダウンロード対象のランIDリスト"
    )


class BatchDownloadEstimate(BaseModel):
    """バッチダウンロード推定サイズレスポンス"""
    run_count: int
    estimated_size: int
    estimated_size_mb: float
    can_download: bool
    message: Optional[str] = None


# ==================== Utility Functions ====================

def sort_files(files: List[dict], sort_by: str, order: str) -> List[dict]:
    """
    ファイルリストをソートする

    Args:
        files: ファイルリスト
        sort_by: ソート対象 ('name', 'size', 'last_modified')
        order: ソート順 ('asc', 'desc')

    Returns:
        ソート済みファイルリスト
    """
    reverse = order == 'desc'

    if sort_by == 'name':
        return sorted(files, key=lambda x: x.get('name', '').lower(), reverse=reverse)
    elif sort_by == 'size':
        return sorted(files, key=lambda x: x.get('size', 0) or 0, reverse=reverse)
    elif sort_by == 'last_modified':
        return sorted(files, key=lambda x: x.get('last_modified', ''), reverse=reverse)
    else:
        return files


# ==================== Endpoints ====================

@router.get("/storage/info", tags=["storage"], response_model=StorageInfoResponse)
async def get_storage_info():
    """
    ストレージモード情報を取得する

    Returns:
        StorageInfoResponse: ストレージモード、バケット名またはローカルパス
    """
    try:
        storage = get_storage()

        if storage.mode == 's3':
            return StorageInfoResponse(
                mode='s3',
                bucket_name=os.getenv('S3_BUCKET_NAME', 'labcode-dev-artifacts'),
                local_path=None
            )
        else:
            return StorageInfoResponse(
                mode='local',
                bucket_name=None,
                local_path=os.getenv('LOCAL_STORAGE_PATH', '/data/storage'),
                db_path=os.getenv('DATABASE_URL', 'sqlite:////data/sql_app.db').replace('sqlite:///', '')
            )
    except Exception as e:
        logger.error(f"Error getting storage info: {e}")
        raise HTTPException(status_code=500, detail="Failed to get storage info")


@router.get("/storage/list", tags=["storage"], response_model=ListResponse)
async def list_files(
    prefix: str = Query(..., description="S3プレフィックス（例: runs/1/）"),
    sort_by: str = Query("name", description="ソート対象: name, size, last_modified"),
    order: str = Query("asc", description="ソート順: asc, desc"),
    page: int = Query(1, ge=1, description="ページ番号"),
    per_page: int = Query(50, ge=1, le=100, description="1ページあたりの件数")
):
    """
    S3バケット内のファイル・フォルダ一覧を取得する

    Args:
        prefix: S3プレフィックス
        sort_by: ソート対象
        order: ソート順
        page: ページ番号
        per_page: 1ページあたりの件数

    Returns:
        ListResponse: ファイル一覧、ディレクトリ一覧、ページネーション情報
    """
    # パラメータバリデーション
    if sort_by not in ['name', 'size', 'last_modified']:
        raise HTTPException(status_code=400, detail="sort_by must be 'name', 'size', or 'last_modified'")
    if order not in ['asc', 'desc']:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")

    try:
        s3 = S3Service()
        response = s3.list_objects(prefix=prefix)

        # ファイル一覧の構築
        files = []
        for obj in response['contents']:
            key = obj['Key']
            # prefixと同一のキーは除外（フォルダ自体）
            if key != prefix and not key.endswith('/'):
                name = key.split('/')[-1]
                extension = name.split('.')[-1].lower() if '.' in name else ''
                files.append({
                    'name': name,
                    'type': 'file',
                    'path': key,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'extension': extension
                })

        # ディレクトリ一覧の構築
        directories = []
        for prefix_info in response['common_prefixes']:
            dir_path = prefix_info['Prefix']
            dir_name = dir_path.rstrip('/').split('/')[-1]
            directories.append({
                'name': dir_name,
                'type': 'directory',
                'path': dir_path
            })

        # ソート
        files = sort_files(files, sort_by, order)

        # ページネーション
        total = len(files)
        start = (page - 1) * per_page
        end = start + per_page
        paginated_files = files[start:end]

        return ListResponse(
            files=[FileItem(**f) for f in paginated_files],
            directories=[DirectoryItem(**d) for d in directories],
            pagination=PaginationInfo(
                total=total,
                page=page,
                per_page=per_page,
                total_pages=(total + per_page - 1) // per_page if total > 0 else 1
            )
        )

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"S3 ClientError: {error_code} - {e}")
        if error_code == 'NoSuchBucket':
            raise HTTPException(status_code=500, detail="Bucket not found")
        elif error_code == 'AccessDenied':
            raise HTTPException(status_code=403, detail="Access denied to S3")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to connect to S3: {error_code}")
    except Exception as e:
        logger.error(f"Unexpected error in list_files: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/storage/preview", tags=["storage"], response_model=PreviewResponse)
async def preview_file(
    file_path: str = Query(..., description="S3キー（例: runs/1/output.json）"),
    max_lines: int = Query(1000, ge=1, le=10000, description="最大行数")
):
    """
    テキストファイルの内容を取得してプレビューする

    Args:
        file_path: S3キー
        max_lines: 最大行数（デフォルト: 1000）

    Returns:
        PreviewResponse: ファイル内容、コンテンツタイプ、サイズ等
    """
    # ファイルタイプ判定
    extension = file_path.split('.')[-1].lower() if '.' in file_path else ''
    content_type = get_content_type(extension)

    if content_type == 'binary':
        raise HTTPException(
            status_code=415,
            detail="Binary files cannot be previewed"
        )

    try:
        s3 = S3Service()
        response = s3.get_object(key=file_path)

        # 内容をデコード
        try:
            content = response['body'].decode('utf-8')
        except UnicodeDecodeError:
            raise HTTPException(
                status_code=415,
                detail="File encoding is not UTF-8, cannot preview"
            )

        # 行数制限
        lines = content.split('\n')
        truncated = len(lines) > max_lines
        if truncated:
            content = '\n'.join(lines[:max_lines])

        return PreviewResponse(
            content=content,
            content_type=content_type,
            size=response['content_length'],
            last_modified=response['last_modified'].isoformat(),
            truncated=truncated
        )

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"S3 ClientError: {error_code} - {e}")
        if error_code == 'NoSuchKey':
            raise HTTPException(status_code=404, detail="File not found")
        elif error_code == 'AccessDenied':
            raise HTTPException(status_code=403, detail="Access denied to S3")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to fetch file: {error_code}")
    except Exception as e:
        logger.error(f"Unexpected error in preview_file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/storage/download", tags=["storage"], response_model=DownloadResponse)
async def download_file(
    file_path: str = Query(..., description="S3キー（例: runs/1/output.json）"),
    expires_in: int = Query(3600, ge=60, le=86400, description="有効期限（秒）")
):
    """
    ダウンロード用の事前署名URLを生成する

    Args:
        file_path: S3キー
        expires_in: 有効期限（秒）、デフォルト3600秒（1時間）

    Returns:
        DownloadResponse: 事前署名URL、有効期限
    """
    try:
        s3 = S3Service()

        # ファイル存在確認
        try:
            s3.head_object(key=file_path)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404' or error_code == 'NoSuchKey':
                raise HTTPException(status_code=404, detail="File not found")
            raise

        # 事前署名URL生成
        url = s3.generate_presigned_url(key=file_path, expires_in=expires_in)

        # 有効期限計算
        expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

        return DownloadResponse(
            download_url=url,
            expires_at=expires_at.isoformat() + 'Z'
        )

    except HTTPException:
        raise
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"S3 ClientError: {error_code} - {e}")
        if error_code == 'AccessDenied':
            raise HTTPException(status_code=403, detail="Access denied to S3")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to generate download URL: {error_code}")
    except Exception as e:
        logger.error(f"Unexpected error in download_file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/storage/download-direct", tags=["storage"])
async def download_file_direct(
    file_path: str = Query(..., description="ファイルパス（例: runs/1/protocol.yaml）")
):
    """
    ファイルを直接ダウンロードする（ローカルモード用）

    Args:
        file_path: ファイルパス

    Returns:
        StreamingResponse: ファイルストリーム
    """
    try:
        s3 = S3Service()

        # ファイル存在確認
        try:
            s3.head_object(key=file_path)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404' or error_code == 'NoSuchKey':
                raise HTTPException(status_code=404, detail="File not found")
            raise

        # ファイル名を抽出
        filename = file_path.split('/')[-1]

        # ストリーミングでファイルを返す
        def file_generator():
            for chunk in s3.get_object_stream(file_path):
                yield chunk

        return StreamingResponse(
            file_generator(),
            media_type='application/octet-stream',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )

    except HTTPException:
        raise
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"Storage ClientError in download_file_direct: {error_code} - {e}")
        if error_code == 'AccessDenied':
            raise HTTPException(status_code=403, detail="Access denied")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to download file: {error_code}")
    except Exception as e:
        logger.error(f"Unexpected error in download_file_direct: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/storage/batch-download", tags=["storage"])
async def batch_download(
    request: BatchDownloadRequest,
    db: Session = Depends(get_db)
):
    """
    複数ランのファイルをZIP形式で一括ダウンロードする

    Args:
        request: バッチダウンロードリクエスト（run_ids）
        db: データベースセッション

    Returns:
        StreamingResponse: ZIPファイルストリーム
    """
    try:
        # ランIDのバリデーション
        if not request.run_ids:
            raise HTTPException(
                status_code=400,
                detail="run_ids is required and must not be empty"
            )

        # データベースからラン情報を取得
        runs = db.query(Run).filter(Run.id.in_(request.run_ids)).all()

        if not runs:
            raise HTTPException(
                status_code=404,
                detail="No runs found for the specified IDs"
            )

        # ラン情報を辞書リストに変換
        runs_data = [
            {
                'id': run.id,
                'storage_address': run.storage_address,
                'file_name': run.file_name,
                'status': run.status
            }
            for run in runs
        ]

        # 見つからなかったランIDを警告
        found_ids = {run.id for run in runs}
        missing_ids = set(request.run_ids) - found_ids
        if missing_ids:
            logger.warning(f"Some runs not found: {missing_ids}")

        # ZIPストリームを生成
        zip_service = ZipStreamService()
        zip_stream = zip_service.create_zip_stream(runs_data)
        filename = zip_service.generate_filename()

        return StreamingResponse(
            zip_stream,
            media_type='application/zip',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )

    except SizeLimitExceededError as e:
        raise HTTPException(status_code=413, detail=str(e))
    except HTTPException:
        raise
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"S3 ClientError in batch_download: {error_code} - {e}")
        if error_code == 'AccessDenied':
            raise HTTPException(status_code=403, detail="Access denied to S3")
        else:
            raise HTTPException(
                status_code=503,
                detail="Failed to connect to storage service"
            )
    except Exception as e:
        logger.error(f"Unexpected error in batch_download: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/storage/batch-download/estimate", tags=["storage"], response_model=BatchDownloadEstimate)
async def estimate_batch_download(
    request: BatchDownloadRequest,
    db: Session = Depends(get_db)
):
    """
    バッチダウンロードの推定サイズを取得する

    Args:
        request: バッチダウンロードリクエスト（run_ids）
        db: データベースセッション

    Returns:
        BatchDownloadEstimate: 推定サイズ情報
    """
    try:
        # データベースからラン情報を取得
        runs = db.query(Run).filter(Run.id.in_(request.run_ids)).all()

        if not runs:
            return BatchDownloadEstimate(
                run_count=0,
                estimated_size=0,
                estimated_size_mb=0.0,
                can_download=False,
                message="No runs found for the specified IDs"
            )

        # ラン情報を辞書リストに変換
        runs_data = [
            {
                'id': run.id,
                'storage_address': run.storage_address
            }
            for run in runs
        ]

        # サイズ推定
        zip_service = ZipStreamService()
        estimated_size = zip_service.estimate_zip_size(
            request.run_ids,
            runs_data
        )

        # 500MB上限チェック
        max_size = 500 * 1024 * 1024
        can_download = estimated_size <= max_size

        message = None
        if not can_download:
            message = f"Estimated size ({estimated_size // (1024*1024)}MB) exceeds limit (500MB)"

        return BatchDownloadEstimate(
            run_count=len(runs),
            estimated_size=estimated_size,
            estimated_size_mb=round(estimated_size / (1024 * 1024), 2),
            can_download=can_download,
            message=message
        )

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"S3 ClientError in estimate_batch_download: {error_code} - {e}")
        raise HTTPException(
            status_code=503,
            detail="Failed to connect to storage service"
        )
    except Exception as e:
        logger.error(f"Unexpected error in estimate_batch_download: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
