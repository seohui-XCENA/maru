# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
import logging
import os
from dataclasses import dataclass, fields
from pathlib import Path

logger = logging.getLogger(__name__)

# Config file: maru package root / config.yaml
# Override with MARU_CONFIG env var.
_PACKAGE_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG_PATH = _PACKAGE_ROOT / "config.yaml"


def _load_maru_yaml() -> dict:
    """Load maru config from YAML file.

    Search order: MARU_CONFIG env → <maru_package_root>/config.yaml
    Returns empty dict if no config file found.
    """
    try:
        import yaml
    except ImportError:
        return {}

    env_path = os.environ.get("MARU_CONFIG")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            with open(p) as f:
                logger.info("Loading maru config from %s", p)
                return yaml.safe_load(f) or {}
        logger.warning("MARU_CONFIG=%s not found, ignoring", env_path)
        return {}

    if _DEFAULT_CONFIG_PATH.is_file():
        with open(_DEFAULT_CONFIG_PATH) as f:
            logger.info("Loading maru config from %s", _DEFAULT_CONFIG_PATH)
            return yaml.safe_load(f) or {}

    return {}


def _parse_env_bool(name: str) -> bool | None:
    """Parse an optional boolean env var.

    Returns:
        - True/False if the env var is set to a recognized boolean value
        - None if the env var is unset

    Raises:
        ValueError: If the env var is set to an invalid boolean value
    """
    raw = os.environ.get(name)
    if raw is None:
        return None

    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise ValueError(
        f"{name} must be one of: 1/0, true/false, yes/no, on/off (got {raw!r})"
    )


@dataclass
class MaruConfig:
    """
    Configuration for Maru client.

    Attributes:
        server_url: URL of the MaruServer (e.g., "tcp://localhost:5555")
        instance_id: Unique identifier for this client instance
        pool_size: Default pool size to request (in bytes)
        auto_connect: Whether to automatically connect on initialization
    """

    # fs/marufs mode — set mount_path to enable; None = RPC mode
    mount_path: str | None = None

    # RPC mode fields
    server_url: str = "tcp://localhost:5555"
    instance_id: str | None = None
    pool_size: int = 1024 * 1024 * 100  # 100MB default
    chunk_size_bytes: int = 1024 * 1024  # 1MB default
    auto_connect: bool = True
    timeout_ms: int = 2000  # Socket timeout in milliseconds
    use_async_rpc: bool = True  # Use async DEALER-ROUTER RPC (RpcAsyncClient)
    max_inflight: int = 64  # Max concurrent in-flight async requests (backpressure)
    eager_map: bool = True  # Pre-map all shared regions on connect

    @property
    def is_marufs_mode(self) -> bool:
        """True if configured for marufs (fs) mode."""
        return self.mount_path is not None

    def __post_init__(self):
        """Generate instance_id if not provided. Validate config.

        Maru-specific fields (mount_path, etc.) are loaded from the YAML
        config file and applied as defaults — only if not explicitly set
        by the caller (i.e., still at dataclass default).
        """
        # Load YAML config for maru-specific fields.
        # Fields explicitly passed by the caller (e.g., from LMCache) take
        # precedence; YAML fills in fields the caller didn't set.
        yaml_cfg = _load_maru_yaml()
        if yaml_cfg:
            field_names = {f.name for f in fields(self)}
            for key, value in yaml_cfg.items():
                if key in field_names and getattr(self, key) is None:
                    setattr(self, key, value)
                    logger.debug("maru config: %s=%s (from yaml)", key, value)

        if self.instance_id is None:
            import uuid

            self.instance_id = str(uuid.uuid4())

        # Optional env override for eager shared-region pre-mapping.
        env_eager_map = _parse_env_bool("MARU_EAGER_MAP")
        if env_eager_map is not None:
            self.eager_map = env_eager_map

        if self.chunk_size_bytes <= 0:
            raise ValueError(
                f"chunk_size_bytes must be positive, got {self.chunk_size_bytes}"
            )
        if self.pool_size < self.chunk_size_bytes:
            raise ValueError(
                f"pool_size ({self.pool_size}) must be >= "
                f"chunk_size_bytes ({self.chunk_size_bytes})"
            )
