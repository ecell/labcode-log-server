from fastapi import FastAPI
from api.route import users, projects, runs, processes, operations, edges, ports, storage, storage_v2, process_operations
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPIライフサイクル管理

    起動時: データベース初期化チェック
    終了時: リソースクリーンアップ
    """
    # === 起動時処理 ===
    logger.info("=== FastAPI Starting ===")

    # DB初期化
    from init_db import ensure_database_ready
    result = ensure_database_ready()

    if not result['success'] and result['action'] != 'none':
        logger.error("データベース初期化に失敗しました")

    logger.info("=== FastAPI Ready ===")

    yield  # アプリケーション実行

    # === 終了時処理 ===
    logger.info("=== FastAPI Shutting Down ===")


app = FastAPI(lifespan=lifespan)
# CORSミドルウェアの設定
app.add_middleware(
    CORSMiddleware,
    # 許可するオリジン（フロントエンドのURL）
    allow_origins=[
        "http://labcode-web-app.com:5173",
        "http://localhost:5173",  # 開発環境用に追加
    ],
    allow_credentials=True,
    allow_methods=["*"],  # 全てのHTTPメソッドを許可
    allow_headers=["*"],  # 全てのヘッダーを許可
)

app.include_router(users.router, prefix="/api")
app.include_router(projects.router, prefix="/api")
app.include_router(runs.router, prefix="/api")
app.include_router(processes.router, prefix="/api")
app.include_router(operations.router, prefix="/api")
app.include_router(edges.router, prefix="/api")
app.include_router(ports.router, prefix="/api")
app.include_router(storage.router, prefix="/api")
app.include_router(process_operations.router, prefix="/api")
# HAL (Hybrid Access Layer) を使用した新API
app.include_router(storage_v2.router)
