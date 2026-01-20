# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test credentials and authentication utilities."""

import base64
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.core.credentials import AccessToken, TokenCredential


def create_dummy_jwt(expiry_timestamp: int) -> str:
    """
    Create a dummy JWT token for testing.

    Args:
        expiry_timestamp: Unix timestamp for token expiry

    Returns:
        A properly formatted JWT token string
    """
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "exp": expiry_timestamp,
        "upn": "test@example.com",
        "aud": "https://api.fabric.microsoft.com",
    }

    header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip("=")
    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    signature = "dummy_signature"

    return f"{header_b64}.{payload_b64}.{signature}"


class DummyTokenCredential(TokenCredential):
    """A static token credential for testing."""

    def __init__(self, expiry_days: int = 365):
        """
        Initialize static token credential.

        Args:
            expiry_days: Number of days until expiry. Defaults to 365.
        """
        self.expiry = int((datetime.now(timezone.utc) + timedelta(days=expiry_days)).timestamp())
        self._token = create_dummy_jwt(self.expiry)
        self.logger = logging.getLogger(__name__)

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:  # noqa: ARG002
        """Get the static access token."""
        self.logger.debug(f"Static token credential - getting token for scopes: {scopes}")
        return AccessToken(self._token, self.expiry)

    def get_expire(self) -> int:
        """Get the token expiry timestamp."""
        return self.expiry
