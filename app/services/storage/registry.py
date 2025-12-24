"""バックエンドレジストリ

ストレージバックエンドの動的登録・取得を管理。
"""

from typing import Dict, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from .backends.base import StorageBackend


class BackendRegistry:
    """ストレージバックエンドのレジストリ"""

    _backends: Dict[str, Type['StorageBackend']] = {}

    @classmethod
    def register(cls, mode: str):
        """
        バックエンドクラスを登録するデコレータ

        使用例:
            @BackendRegistry.register("s3")
            class S3StorageBackend(StorageBackend):
                ...
        """
        def decorator(backend_class: Type['StorageBackend']):
            cls._backends[mode.lower()] = backend_class
            return backend_class
        return decorator

    @classmethod
    def get(cls, mode: str) -> Type['StorageBackend']:
        """
        モード名からバックエンドクラスを取得

        Args:
            mode: ストレージモード名（'s3', 'local'等）

        Returns:
            バックエンドクラス

        Raises:
            ValueError: 未登録のモードが指定された場合
        """
        mode_lower = mode.lower()
        if mode_lower not in cls._backends:
            available = ", ".join(cls._backends.keys())
            raise ValueError(f"Unknown storage mode: {mode}. Available: {available}")
        return cls._backends[mode_lower]

    @classmethod
    def list_modes(cls) -> list:
        """登録済みモード一覧を取得"""
        return list(cls._backends.keys())

    @classmethod
    def is_registered(cls, mode: str) -> bool:
        """モードが登録済みか確認"""
        return mode.lower() in cls._backends

    @classmethod
    def clear(cls):
        """テスト用: レジストリをクリア"""
        cls._backends.clear()
