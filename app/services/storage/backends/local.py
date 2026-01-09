"""ローカルファイルシステムストレージバックエンド

ローカルファイルシステムを使用したストレージ。
"""

import logging
from pathlib import Path
from typing import List, Optional, Generator, Dict, Any
from datetime import datetime

from ..registry import BackendRegistry
from ..config import LocalConfig
from .base import StorageBackend

logger = logging.getLogger(__name__)


@BackendRegistry.register("local")
class LocalStorageBackend(StorageBackend):
    """ローカルファイルシステムストレージバックエンド"""

    def __init__(self, config: LocalConfig = None):
        """
        ローカルバックエンドを初期化

        Args:
            config: ローカル設定。Noneの場合は環境変数から読み込み
        """
        if config is None:
            config = LocalConfig.from_env()

        self.base_path = Path(config.base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalStorageBackend initialized: path={self.base_path}")

    def _get_full_path(self, path: str) -> Path:
        """相対パスをフルパスに変換"""
        return self.base_path / path

    def load(self, path: str) -> Optional[bytes]:
        try:
            full_path = self._get_full_path(path)
            with open(full_path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
            logger.debug(f"Local file not found: {path}")
            return None
        except Exception as e:
            logger.error(f"Local load failed: {path} - {e}")
            return None

    def load_stream(self, path: str, chunk_size: int = 65536) -> Generator[bytes, None, None]:
        try:
            full_path = self._get_full_path(path)
            with open(full_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
        except Exception as e:
            logger.error(f"Local stream load failed: {path} - {e}")
            return

    def list_objects(self, prefix: str) -> List[Dict[str, Any]]:
        all_objects = []
        base_dir = self._get_full_path(prefix)

        if not base_dir.exists():
            return []

        try:
            for file_path in base_dir.rglob('*'):
                if file_path.is_file():
                    relative_path = str(file_path.relative_to(self.base_path))
                    stat = file_path.stat()
                    all_objects.append({
                        'Key': relative_path,
                        'Size': stat.st_size,
                        'LastModified': datetime.fromtimestamp(stat.st_mtime)
                    })
        except Exception as e:
            logger.error(f"Local list_objects failed: {prefix} - {e}")

        return all_objects

    def list_objects_with_dirs(self, prefix: str, delimiter: str = '/') -> Dict[str, Any]:
        base_dir = self._get_full_path(prefix)
        contents = []
        common_prefixes = []

        if not base_dir.exists():
            return {'contents': contents, 'common_prefixes': common_prefixes}

        try:
            for item in base_dir.iterdir():
                relative_path = str(item.relative_to(self.base_path))
                if item.is_file():
                    stat = item.stat()
                    contents.append({
                        'Key': relative_path,
                        'Size': stat.st_size,
                        'LastModified': datetime.fromtimestamp(stat.st_mtime)
                    })
                elif item.is_dir():
                    common_prefixes.append({
                        'Prefix': relative_path + '/'
                    })
        except Exception as e:
            logger.error(f"Local list_objects_with_dirs failed: {prefix} - {e}")

        return {'contents': contents, 'common_prefixes': common_prefixes}

    def exists(self, path: str) -> bool:
        full_path = self._get_full_path(path)
        return full_path.exists() and full_path.is_file()

    def get_metadata(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            full_path = self._get_full_path(path)
            if not full_path.exists():
                return None
            stat = full_path.stat()
            return {
                'content_length': stat.st_size,
                'last_modified': datetime.fromtimestamp(stat.st_mtime),
                'content_type': 'application/octet-stream'
            }
        except Exception:
            return None

    def save(self, path: str, content: bytes, content_type: str = 'application/octet-stream') -> bool:
        try:
            full_path = self._get_full_path(path)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            with open(full_path, 'wb') as f:
                f.write(content)
            logger.debug(f"Local save success: {path}")
            return True
        except Exception as e:
            logger.error(f"Local save failed: {path} - {e}")
            return False

    def delete(self, path: str) -> bool:
        try:
            full_path = self._get_full_path(path)
            if full_path.exists():
                full_path.unlink()
            return True
        except Exception as e:
            logger.error(f"Local delete failed: {path} - {e}")
            return False

    def generate_presigned_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        # ローカルモードでは事前署名URLをサポートしない
        logger.warning(f"Presigned URL not supported in local mode: {path}")
        return None
