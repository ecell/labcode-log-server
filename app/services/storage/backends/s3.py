"""S3ストレージバックエンド

AWS S3およびS3互換ストレージ（MinIO等）に対応。
"""

import logging
from typing import List, Optional, Generator, Dict, Any

import boto3
from botocore.exceptions import ClientError

from ..registry import BackendRegistry
from ..config import S3Config
from .base import StorageBackend

logger = logging.getLogger(__name__)


@BackendRegistry.register("s3")
class S3StorageBackend(StorageBackend):
    """S3ストレージバックエンド"""

    def __init__(self, config: S3Config = None):
        """
        S3バックエンドを初期化

        Args:
            config: S3設定。Noneの場合は環境変数から読み込み
        """
        if config is None:
            config = S3Config.from_env()

        client_kwargs = {
            'aws_access_key_id': config.access_key_id,
            'aws_secret_access_key': config.secret_access_key,
            'region_name': config.region
        }

        if config.endpoint_url:
            client_kwargs['endpoint_url'] = config.endpoint_url

        self.client = boto3.client('s3', **client_kwargs)
        self.bucket_name = config.bucket_name
        logger.info(f"S3StorageBackend initialized: bucket={self.bucket_name}")

    def load(self, path: str) -> Optional[bytes]:
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=path)
            return response['Body'].read()
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchKey':
                logger.debug(f"S3 object not found: {path}")
            else:
                logger.error(f"S3 load failed: {path} - {e}")
            return None

    def load_stream(self, path: str, chunk_size: int = 65536) -> Generator[bytes, None, None]:
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=path)
            body = response['Body']
            while True:
                chunk = body.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        except ClientError as e:
            logger.error(f"S3 stream load failed: {path} - {e}")
            return

    def list_objects(self, prefix: str) -> List[Dict[str, Any]]:
        all_objects = []
        continuation_token = None

        while True:
            params = {
                'Bucket': self.bucket_name,
                'Prefix': prefix,
            }
            if continuation_token:
                params['ContinuationToken'] = continuation_token

            try:
                response = self.client.list_objects_v2(**params)
            except ClientError as e:
                logger.error(f"S3 list_objects failed: {prefix} - {e}")
                return []

            contents = response.get('Contents', [])
            for obj in contents:
                if obj['Key'] != prefix and not obj['Key'].endswith('/'):
                    all_objects.append({
                        'Key': obj['Key'],
                        'Size': obj['Size'],
                        'LastModified': obj['LastModified']
                    })

            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break

        return all_objects

    def list_objects_with_dirs(self, prefix: str, delimiter: str = '/') -> Dict[str, Any]:
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter=delimiter
            )
            return {
                'contents': response.get('Contents', []),
                'common_prefixes': response.get('CommonPrefixes', [])
            }
        except ClientError as e:
            logger.error(f"S3 list_objects_with_dirs failed: {prefix} - {e}")
            return {'contents': [], 'common_prefixes': []}

    def exists(self, path: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=path)
            return True
        except ClientError:
            return False

    def get_metadata(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.client.head_object(Bucket=self.bucket_name, Key=path)
            return {
                'content_length': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType', 'application/octet-stream')
            }
        except ClientError:
            return None

    def save(self, path: str, content: bytes, content_type: str = 'application/octet-stream') -> bool:
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=path,
                Body=content,
                ContentType=content_type
            )
            logger.debug(f"S3 upload success: {path}")
            return True
        except ClientError as e:
            logger.error(f"S3 upload failed: {path} - {e}")
            return False

    def delete(self, path: str) -> bool:
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=path)
            return True
        except ClientError as e:
            logger.error(f"S3 delete failed: {path} - {e}")
            return False

    def generate_presigned_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': path
                },
                ExpiresIn=expires_in
            )
            return url
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {path} - {e}")
            return None
