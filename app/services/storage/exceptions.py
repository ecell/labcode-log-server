"""カスタム例外

ストレージ関連のエラーを表す例外クラス。
"""


class StorageError(Exception):
    """ストレージ操作の基底例外"""
    pass


class StorageNotFoundError(StorageError):
    """ファイルが見つからない"""
    pass


class StorageAccessError(StorageError):
    """ストレージアクセスエラー"""
    pass


class StorageConfigError(StorageError):
    """設定エラー"""
    pass


class BackendNotRegisteredError(StorageError):
    """バックエンドが未登録"""
    pass
