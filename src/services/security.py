import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.services.audit import audit_logger


class EncryptionConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class EncryptedPayload:
    key_id: str
    nonce: str
    ciphertext: str


class SensitiveDataProtector:
    """
    AES-GCM helper for transcript/PII payloads.

    The service is intentionally optional in local tests: if no key is
    configured, payloads pass through unchanged with explicit metadata. In
    production, set POSTCALL_ENCRYPTION_KEY_B64 to a 32-byte base64 key.
    """

    def __init__(
        self,
        key_b64: Optional[str] = None,
        key_id: str = "postcall-local",
    ):
        self._key_b64 = key_b64 if key_b64 is not None else os.getenv("POSTCALL_ENCRYPTION_KEY_B64")
        self._key_id = os.getenv("POSTCALL_ENCRYPTION_KEY_ID", key_id)

    @property
    def enabled(self) -> bool:
        return bool(self._key_b64)

    def protect_json(self, payload: Dict[str, Any], *, interaction_id: str) -> Dict[str, Any]:
        if not self.enabled:
            return {"encrypted": False, "payload": payload}

        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as exc:
            raise EncryptionConfigurationError(
                "cryptography is required when POSTCALL_ENCRYPTION_KEY_B64 is configured"
            ) from exc

        key = base64.b64decode(self._key_b64 or "")
        if len(key) != 32:
            raise EncryptionConfigurationError("POSTCALL_ENCRYPTION_KEY_B64 must decode to 32 bytes")

        nonce = os.urandom(12)
        plaintext = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        aad = interaction_id.encode("utf-8")
        ciphertext = AESGCM(key).encrypt(nonce, plaintext, aad)
        audit_logger.emit(
            "sensitive_payload_encrypted",
            interaction_id=interaction_id,
            key_id=self._key_id,
        )
        return {
            "encrypted": True,
            "algorithm": "AES-256-GCM",
            "key_id": self._key_id,
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        }

    def reveal_json(self, protected_payload: Dict[str, Any], *, interaction_id: str) -> Dict[str, Any]:
        if not protected_payload.get("encrypted"):
            return protected_payload.get("payload", protected_payload)

        if not self.enabled:
            raise EncryptionConfigurationError("encrypted payload cannot be decrypted without configured key")

        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as exc:
            raise EncryptionConfigurationError(
                "cryptography is required when POSTCALL_ENCRYPTION_KEY_B64 is configured"
            ) from exc

        key = base64.b64decode(self._key_b64 or "")
        nonce = base64.b64decode(protected_payload["nonce"])
        ciphertext = base64.b64decode(protected_payload["ciphertext"])
        plaintext = AESGCM(key).decrypt(nonce, ciphertext, interaction_id.encode("utf-8"))
        return json.loads(plaintext.decode("utf-8"))


sensitive_data_protector = SensitiveDataProtector()
