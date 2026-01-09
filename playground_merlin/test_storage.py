"""ストレージAPIのユニットテスト

テスト対象:
- GET /api/storage/list
- GET /api/storage/preview
- GET /api/storage/download
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from fastapi.testclient import TestClient
from botocore.exceptions import ClientError

# テスト用のmainをインポート
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app'))

from main import app

client = TestClient(app)


# ==================== Mock Data ====================

def create_mock_s3_list_response():
    """list_objects_v2のモックレスポンス"""
    return {
        'contents': [
            {
                'Key': 'runs/1/output.json',
                'Size': 1024,
                'LastModified': datetime(2025, 12, 15, 10, 0, 0)
            },
            {
                'Key': 'runs/1/protocol.yaml',
                'Size': 512,
                'LastModified': datetime(2025, 12, 15, 9, 0, 0)
            }
        ],
        'common_prefixes': [
            {'Prefix': 'runs/1/artifacts/'}
        ]
    }


def create_mock_s3_get_response():
    """get_objectのモックレスポンス"""
    return {
        'body': b'{"result": "success", "data": [1, 2, 3]}',
        'content_length': 42,
        'last_modified': datetime(2025, 12, 15, 10, 0, 0)
    }


def create_mock_s3_head_response():
    """head_objectのモックレスポンス"""
    return {
        'content_length': 1024,
        'last_modified': datetime(2025, 12, 15, 10, 0, 0)
    }


# ==================== GET /api/storage/list Tests ====================

class TestStorageList:
    """GET /api/storage/list のテスト"""

    @patch('api.route.storage.S3Service')
    def test_list_files_success(self, mock_s3_class):
        """正常系: ファイル一覧取得成功"""
        mock_s3 = MagicMock()
        mock_s3.list_objects.return_value = create_mock_s3_list_response()
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/list?prefix=runs/1/")

        assert response.status_code == 200
        data = response.json()
        assert 'files' in data
        assert 'directories' in data
        assert 'pagination' in data
        assert len(data['files']) == 2
        assert len(data['directories']) == 1
        assert data['files'][0]['name'] == 'output.json'
        assert data['directories'][0]['name'] == 'artifacts'

    @patch('api.route.storage.S3Service')
    def test_list_files_empty(self, mock_s3_class):
        """正常系: 空のディレクトリ"""
        mock_s3 = MagicMock()
        mock_s3.list_objects.return_value = {'contents': [], 'common_prefixes': []}
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/list?prefix=runs/empty/")

        assert response.status_code == 200
        data = response.json()
        assert len(data['files']) == 0
        assert len(data['directories']) == 0

    @patch('api.route.storage.S3Service')
    def test_list_files_sort_by_size(self, mock_s3_class):
        """正常系: サイズでソート"""
        mock_s3 = MagicMock()
        mock_s3.list_objects.return_value = create_mock_s3_list_response()
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/list?prefix=runs/1/&sort_by=size&order=desc")

        assert response.status_code == 200
        data = response.json()
        # サイズ降順: output.json (1024) > protocol.yaml (512)
        assert data['files'][0]['name'] == 'output.json'
        assert data['files'][1]['name'] == 'protocol.yaml'

    def test_list_files_missing_prefix(self):
        """異常系: prefix未指定"""
        response = client.get("/api/storage/list")

        assert response.status_code == 422  # Validation error

    @patch('api.route.storage.S3Service')
    def test_list_files_invalid_sort_by(self, mock_s3_class):
        """異常系: 無効なsort_by"""
        response = client.get("/api/storage/list?prefix=runs/1/&sort_by=invalid")

        assert response.status_code == 400
        assert "sort_by" in response.json()['detail']

    @patch('api.route.storage.S3Service')
    def test_list_files_s3_error(self, mock_s3_class):
        """異常系: S3エラー"""
        mock_s3 = MagicMock()
        mock_s3.list_objects.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            'ListObjectsV2'
        )
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/list?prefix=runs/1/")

        assert response.status_code == 403


# ==================== GET /api/storage/preview Tests ====================

class TestStoragePreview:
    """GET /api/storage/preview のテスト"""

    @patch('api.route.storage.S3Service')
    def test_preview_json_success(self, mock_s3_class):
        """正常系: JSONファイルプレビュー"""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = create_mock_s3_get_response()
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/preview?file_path=runs/1/output.json")

        assert response.status_code == 200
        data = response.json()
        assert data['content_type'] == 'json'
        assert 'result' in data['content']
        assert data['truncated'] is False

    @patch('api.route.storage.S3Service')
    def test_preview_yaml_success(self, mock_s3_class):
        """正常系: YAMLファイルプレビュー"""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            'body': b'key: value\nlist:\n  - item1\n  - item2',
            'content_length': 35,
            'last_modified': datetime(2025, 12, 15, 10, 0, 0)
        }
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/preview?file_path=runs/1/config.yaml")

        assert response.status_code == 200
        data = response.json()
        assert data['content_type'] == 'yaml'

    @patch('api.route.storage.S3Service')
    def test_preview_truncated(self, mock_s3_class):
        """正常系: 行数制限による切り詰め"""
        mock_s3 = MagicMock()
        # 100行のテストデータ
        content = '\n'.join([f'line {i}' for i in range(100)])
        mock_s3.get_object.return_value = {
            'body': content.encode('utf-8'),
            'content_length': len(content),
            'last_modified': datetime(2025, 12, 15, 10, 0, 0)
        }
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/preview?file_path=runs/1/log.txt&max_lines=50")

        assert response.status_code == 200
        data = response.json()
        assert data['truncated'] is True
        assert len(data['content'].split('\n')) == 50

    def test_preview_binary_file(self):
        """異常系: バイナリファイル"""
        response = client.get("/api/storage/preview?file_path=runs/1/data.bin")

        assert response.status_code == 415
        assert "Binary" in response.json()['detail']

    @patch('api.route.storage.S3Service')
    def test_preview_file_not_found(self, mock_s3_class):
        """異常系: ファイルが存在しない"""
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey', 'Message': 'Not Found'}},
            'GetObject'
        )
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/preview?file_path=runs/1/nonexistent.json")

        assert response.status_code == 404


# ==================== GET /api/storage/download Tests ====================

class TestStorageDownload:
    """GET /api/storage/download のテスト"""

    @patch('api.route.storage.S3Service')
    def test_download_success(self, mock_s3_class):
        """正常系: ダウンロードURL生成"""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = create_mock_s3_head_response()
        mock_s3.generate_presigned_url.return_value = 'https://example.s3.amazonaws.com/runs/1/output.json?signature=xxx'
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/download?file_path=runs/1/output.json")

        assert response.status_code == 200
        data = response.json()
        assert 'download_url' in data
        assert 'expires_at' in data
        assert 's3.amazonaws.com' in data['download_url']

    @patch('api.route.storage.S3Service')
    def test_download_custom_expiry(self, mock_s3_class):
        """正常系: カスタム有効期限"""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = create_mock_s3_head_response()
        mock_s3.generate_presigned_url.return_value = 'https://example.s3.amazonaws.com/test'
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/download?file_path=runs/1/output.json&expires_in=7200")

        assert response.status_code == 200
        mock_s3.generate_presigned_url.assert_called_once()
        call_args = mock_s3.generate_presigned_url.call_args
        assert call_args[1]['expires_in'] == 7200

    @patch('api.route.storage.S3Service')
    def test_download_file_not_found(self, mock_s3_class):
        """異常系: ファイルが存在しない"""
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey', 'Message': 'Not Found'}},
            'HeadObject'
        )
        mock_s3_class.return_value = mock_s3

        response = client.get("/api/storage/download?file_path=runs/1/nonexistent.json")

        assert response.status_code == 404

    def test_download_expires_in_too_short(self):
        """異常系: 有効期限が短すぎる"""
        response = client.get("/api/storage/download?file_path=runs/1/output.json&expires_in=30")

        assert response.status_code == 422  # Validation error


# ==================== Integration Tests (Optional) ====================

class TestStorageIntegration:
    """統合テスト（実際のS3接続が必要）

    Note: これらのテストは環境変数が設定されている場合のみ実行される
    """

    @pytest.mark.skipif(
        not os.getenv('AWS_ACCESS_KEY_ID'),
        reason="AWS credentials not configured"
    )
    def test_real_s3_list(self):
        """実際のS3へのリスト操作"""
        response = client.get("/api/storage/list?prefix=")

        assert response.status_code == 200


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
