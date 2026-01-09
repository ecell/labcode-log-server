"""統合ストレージサービス

ストレージバックエンドを抽象化し、統一的なAPIを提供。
責任分離型設計: Read + 管理機能を提供（Write操作はオプショナル）
"""

import logging
from typing import Optional, List, Dict, Any, Generator

from .config import StorageConfig
from .registry import BackendRegistry
from .backends.base import StorageBackend

logger = logging.getLogger(__name__)


class StorageService:
    """
    統合ストレージサービス（シングルトン）

    環境変数STORAGE_MODEでバックエンドを切り替え:
    - 's3': S3ストレージ（デフォルト）
    - 'local': ローカルファイルシステム
    """

    _instance: Optional['StorageService'] = None
    _config: Optional[StorageConfig] = None

    def __new__(cls, config: Optional[StorageConfig] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(config)
        return cls._instance

    def _initialize(self, config: Optional[StorageConfig] = None):
        """バックエンドを初期化"""
        self._config = config or StorageConfig.from_env()

        # レジストリからバックエンドクラスを取得
        backend_class = BackendRegistry.get(self._config.mode)

        # バックエンド固有設定を取得
        backend_config = self._config.get_backend_config()

        # バックエンドをインスタンス化
        self._backend = backend_class(backend_config)

        self.mode = self._config.mode
        logger.info(f"StorageService initialized: mode={self.mode}")

    @property
    def backend(self) -> StorageBackend:
        """バックエンドインスタンスを取得"""
        return self._backend

    @property
    def config(self) -> StorageConfig:
        """設定を取得"""
        return self._config

    # --- 読み取り系メソッド ---

    def load(self, path: str) -> Optional[bytes]:
        """ファイルを読み込み"""
        return self._backend.load(path)

    def load_text(self, path: str, encoding: str = 'utf-8') -> Optional[str]:
        """テキストファイルを読み込み"""
        content = self.load(path)
        if content is None:
            return None
        return content.decode(encoding)

    def load_json(self, path: str) -> Optional[dict]:
        """JSONファイルを読み込み"""
        import json
        text = self.load_text(path)
        if text is None:
            return None
        return json.loads(text)

    def load_stream(self, path: str, chunk_size: int = 65536) -> Generator[bytes, None, None]:
        """ファイルをストリーミング読み込み"""
        return self._backend.load_stream(path, chunk_size)

    def list_objects(self, prefix: str) -> List[Dict[str, Any]]:
        """オブジェクト一覧を取得"""
        return self._backend.list_objects(prefix)

    def list_objects_with_dirs(self, prefix: str, delimiter: str = '/') -> Dict[str, Any]:
        """ファイルとディレクトリの一覧を取得"""
        return self._backend.list_objects_with_dirs(prefix, delimiter)

    def exists(self, path: str) -> bool:
        """ファイル存在確認"""
        return self._backend.exists(path)

    def get_metadata(self, path: str) -> Optional[Dict[str, Any]]:
        """ファイルメタデータ取得"""
        return self._backend.get_metadata(path)

    def calculate_total_size(self, prefix: str) -> int:
        """指定プレフィックス配下の合計サイズを計算"""
        objects = self.list_objects(prefix)
        return sum(obj['Size'] for obj in objects)

    # --- 書き込み系メソッド（オプショナル） ---

    def save(self, path: str, content: bytes, content_type: str = 'application/octet-stream') -> bool:
        """ファイルを保存（オプショナル: log-serverでは基本使用しない）"""
        return self._backend.save(path, content, content_type)

    def save_text(self, path: str, content: str, encoding: str = 'utf-8') -> bool:
        """テキストファイルを保存（オプショナル）"""
        return self.save(path, content.encode(encoding), content_type='text/plain')

    def save_json(self, path: str, data: dict) -> bool:
        """JSONファイルを保存（オプショナル）"""
        import json
        content = json.dumps(data, ensure_ascii=False, indent=2)
        return self.save(path, content.encode('utf-8'), content_type='application/json')

    def delete(self, path: str) -> bool:
        """ファイル削除（オプショナル）"""
        return self._backend.delete(path)

    # --- S3固有メソッド ---

    def generate_presigned_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        """事前署名URLを生成（S3のみ）"""
        return self._backend.generate_presigned_url(path, expires_in)

    # --- ユーティリティ ---

    @classmethod
    def reset_instance(cls):
        """
        シングルトンインスタンスをリセット（テスト用）

        注意: 本番環境では使用しないこと
        """
        cls._instance = None
        cls._config = None


def get_storage(config: Optional[StorageConfig] = None) -> StorageService:
    """StorageServiceのシングルトンインスタンスを取得"""
    return StorageService(config)
