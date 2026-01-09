#!/usr/bin/env python3
"""S3接続テストスクリプト"""

import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def test_s3_connection():
    """S3への接続をテストする"""
    print("=" * 60)
    print("S3接続テスト")
    print("=" * 60)

    # 環境変数確認
    print("\n📋 環境変数確認:")
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-1')
    bucket_name = os.getenv('S3_BUCKET_NAME', 'labcode-dev-artifacts')

    print(f"  AWS_ACCESS_KEY_ID: {'設定済み (' + access_key[:8] + '...)' if access_key else '❌ 未設定'}")
    print(f"  AWS_SECRET_ACCESS_KEY: {'設定済み' if secret_key else '❌ 未設定'}")
    print(f"  AWS_DEFAULT_REGION: {region}")
    print(f"  S3_BUCKET_NAME: {bucket_name}")

    if not access_key or not secret_key:
        print("\n❌ AWS認証情報が設定されていません")
        print("  → .envファイルを確認してください")
        return False

    try:
        # S3クライアント作成
        print("\n🔍 S3クライアントを作成中...")
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        print("✅ S3クライアント作成成功")

        # バケット存在確認
        print(f"\n🔍 バケット '{bucket_name}' への接続を確認中...")
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ バケット '{bucket_name}' への接続成功")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"❌ バケットが存在しません: {bucket_name}")
                # バケット一覧を確認
                print("\n🔍 アクセス可能なバケット一覧を確認中...")
                try:
                    response = s3_client.list_buckets()
                    buckets = response.get('Buckets', [])
                    if buckets:
                        print("  利用可能なバケット:")
                        for b in buckets:
                            print(f"    - {b['Name']}")
                    else:
                        print("  利用可能なバケットがありません")
                except Exception as e2:
                    print(f"  バケット一覧取得失敗: {e2}")
                return False
            elif error_code == '403':
                print(f"❌ バケットへのアクセスが拒否されました")
                return False
            else:
                raise

        # オブジェクト一覧取得テスト
        print("\n🔍 オブジェクト一覧を取得中...")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=10
        )
        key_count = response.get('KeyCount', 0)
        print(f"✅ オブジェクト一覧取得成功（{key_count}件）")

        if key_count > 0:
            print("\n📁 既存オブジェクト一覧（最大10件）:")
            for obj in response.get('Contents', []):
                size_kb = obj['Size'] / 1024
                print(f"  - {obj['Key']} ({size_kb:.1f} KB)")

        # テストファイルアップロード
        print("\n🔍 テストファイルのアップロードを試行中...")
        test_key = "test/connection_test.txt"
        test_content = f"S3接続テスト成功 - LabCode\nタイムスタンプ: {os.popen('date').read().strip()}"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content.encode('utf-8'),
            ContentType='text/plain; charset=utf-8'
        )
        print(f"✅ テストファイルアップロード成功: {test_key}")

        # テストファイル読み取り
        print("\n🔍 テストファイルの読み取りを試行中...")
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read().decode('utf-8')
        print(f"✅ テストファイル読み取り成功:")
        print(f"   内容: '{content}'")

        # テストファイル削除
        print("\n🔍 テストファイルの削除を試行中...")
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        print(f"✅ テストファイル削除成功")

        # 署名付きURL生成テスト
        print("\n🔍 署名付きURL生成テスト...")
        # ダミーファイルをアップロード
        s3_client.put_object(Bucket=bucket_name, Key="test/presigned_test.txt", Body=b"test")
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': 'test/presigned_test.txt'},
            ExpiresIn=60
        )
        print(f"✅ 署名付きURL生成成功")
        print(f"   URL: {presigned_url[:80]}...")
        # クリーンアップ
        s3_client.delete_object(Bucket=bucket_name, Key="test/presigned_test.txt")

        print("\n" + "=" * 60)
        print("🎉 すべてのS3テストが成功しました！")
        print("=" * 60)

        print("\n📊 テスト結果サマリー:")
        print("  ✅ S3クライアント作成: 成功")
        print("  ✅ バケット接続: 成功")
        print("  ✅ オブジェクト一覧取得: 成功")
        print("  ✅ ファイルアップロード: 成功")
        print("  ✅ ファイル読み取り: 成功")
        print("  ✅ ファイル削除: 成功")
        print("  ✅ 署名付きURL生成: 成功")

        return True

    except NoCredentialsError:
        print("\n❌ AWS認証情報が見つかりません")
        print("  → .envファイルを確認してください")
        print("  → AWS_ACCESS_KEY_ID と AWS_SECRET_ACCESS_KEY が設定されているか確認")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"\n❌ AWSエラー ({error_code}): {error_message}")

        if error_code == 'InvalidAccessKeyId':
            print("  → アクセスキーIDが無効です")
            print("  → AWS_ACCESS_KEY_ID を確認してください")
        elif error_code == 'SignatureDoesNotMatch':
            print("  → シークレットアクセスキーが無効です")
            print("  → AWS_SECRET_ACCESS_KEY を確認してください")
        elif error_code == 'AccessDenied':
            print("  → アクセスが拒否されました")
            print("  → IAM権限を確認してください")

        return False
    except Exception as e:
        print(f"\n❌ 予期せぬエラー: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    # .envファイルを読み込む
    try:
        from dotenv import load_dotenv
        # コンテナ内のパスで.envを探す
        env_paths = [
            '/app/.env',
            '.env',
            '../.env',
            '/home/ayumu/Documents/Science-Aid/SciAid-LabCode/labcode-test-environment/labcode-log-server/.env'
        ]
        for env_path in env_paths:
            if os.path.exists(env_path):
                print(f"📂 .envファイルを読み込み: {env_path}")
                load_dotenv(env_path)
                break
        else:
            print("⚠️ .envファイルが見つかりませんでした（環境変数から読み込み）")
    except ImportError:
        print("⚠️ python-dotenvがインストールされていません（環境変数から読み込み）")

    success = test_s3_connection()
    exit(0 if success else 1)
