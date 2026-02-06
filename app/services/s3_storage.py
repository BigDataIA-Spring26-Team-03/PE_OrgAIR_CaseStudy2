from __future__ import annotations

import io
import json
import gzip
import logging
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from app.config import settings

logger = logging.getLogger(__name__)


class S3Storage:
    """
    S3 artifact storage layer.

    Rules:
    - S3 stores artifacts only (raw / parsed / processed)
    - Snowflake is the source of truth for state
    - Supports both plain text and gzip transparently
    - Never reconstructs logical state from S3
    """

    def __init__(self) -> None:
        # ✅ Uses Settings-derived bucket so S3_BUCKET or S3_BUCKET_NAME both work
        self.bucket = settings.resolved_s3_bucket
        self.prefix = (settings.s3_prefix or "").strip("/")

        self.client = boto3.client(
            "s3",
            region_name=settings.resolved_aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )

    # -------------------------
    # Internal helpers
    # -------------------------
    def _full_key(self, key: str) -> str:
        key = key.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{key}"
        return key

    # -------------------------
    # Write operations
    # -------------------------
    def put_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> str:
        full_key = self._full_key(key)

        extra = {}
        if content_type:
            extra["ContentType"] = content_type

        self.client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=data,
            **extra,
        )
        return full_key

    def put_text(self, key: str, text: str, gzip_compress: bool = False) -> str:
        if gzip_compress:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                gz.write(text.encode("utf-8", errors="ignore"))
            return self.put_bytes(key, buf.getvalue(), content_type="text/plain")

        return self.put_bytes(
            key=key,
            data=text.encode("utf-8", errors="ignore"),
            content_type="text/plain",
        )

    def put_json(self, key: str, obj: Dict[str, Any], gzip_compress: bool = False) -> str:
        """
        Writes JSON, optionally gzip-compressed.
        ✅ Fixed bug: correct call to put_bytes(key=..., data=...)
        """
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8", errors="ignore")

        if gzip_compress:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                gz.write(payload)
            return self.put_bytes(key, buf.getvalue(), content_type="application/json")

        return self.put_bytes(key=key, data=payload, content_type="application/json")

    # -------------------------
    # Read operations
    # -------------------------
    def exists(self, key: str) -> bool:
        """
        Check if an object exists in S3.
        Used heavily for idempotency checks.
        """
        full_key = self._full_key(key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def get_bytes(self, key: str) -> bytes:
        full_key = self._full_key(key)
        resp = self.client.get_object(Bucket=self.bucket, Key=full_key)
        return resp["Body"].read()

    def read_text_auto(self, key: str) -> str:
        """
        Reads either plain text or gzip content safely.
        - Detects gzip via magic bytes OR .gz suffix
        - Never assumes extension correctness
        """
        data = self.get_bytes(key)

        is_gz = (
            key.endswith(".gz")
            or (len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B)
        )

        if is_gz:
            try:
                return gzip.decompress(data).decode("utf-8", errors="ignore")
            except Exception:
                logger.warning("Failed gzip decode for %s, falling back to raw decode", key)
                return data.decode("utf-8", errors="ignore")

        return data.decode("utf-8", errors="ignore")

    def read_json_auto(self, key: str) -> Dict[str, Any]:
        """
        Reads JSON or gzip-compressed JSON transparently.
        """
        data = self.get_bytes(key)

        is_gz = (
            key.endswith(".gz")
            or (len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B)
        )

        if is_gz:
            try:
                data = gzip.decompress(data)
            except Exception:
                logger.warning("Failed gzip decode for %s, using raw bytes", key)

        return json.loads(data.decode("utf-8", errors="ignore"))
