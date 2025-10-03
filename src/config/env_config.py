"""
Configuration module for TikTok Monitoring Worker
"""
import os
from typing import Union, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class EnvConfig:
    """Central configuration class for monitoring worker environment variables"""

    @staticmethod
    def get_bool(key: str, default: bool = False) -> bool:
        """Get boolean value from environment variable"""
        value = os.getenv(key, str(default)).lower()
        return value in ('true', '1', 'yes', 'on')

    @staticmethod
    def get_int(key: str, default: int) -> int:
        """Get integer value from environment variable"""
        try:
            return int(os.getenv(key, str(default)))
        except ValueError:
            return default

    @staticmethod
    def get_float(key: str, default: float) -> float:
        """Get float value from environment variable"""
        try:
            return float(os.getenv(key, str(default)))
        except ValueError:
            return default

    @staticmethod
    def get_str(key: str, default: str = "") -> str:
        """Get string value from environment variable"""
        return os.getenv(key, default)

    # Worker Configuration
    @property
    def worker_id(self) -> str:
        worker_id = self.get_str("MONITORING_WORKER_ID", "")
        if not worker_id:
            raise ValueError(
                "MONITORING_WORKER_ID is required in .env file.\n"
                "Example: MONITORING_WORKER_ID=monitoring_worker_01\n"
                "This ensures worker can recover jobs after restart."
            )
        return worker_id

    @property
    def worker_heartbeat_interval_seconds(self) -> int:
        return self.get_int("WORKER_HEARTBEAT_INTERVAL_SECONDS", 30)

    @property
    def worker_heartbeat_timeout_seconds(self) -> int:
        return self.get_int("WORKER_HEARTBEAT_TIMEOUT_SECONDS", 90)

    @property
    def max_concurrent_monitoring_jobs(self) -> int:
        return self.get_int("MAX_CONCURRENT_MONITORING_JOBS", 100)

    # Monitoring Configuration
    @property
    def monitoring_interval_seconds(self) -> int:
        return self.get_int("MONITORING_INTERVAL_SECONDS", 30)

    @property
    def live_detection_interval_seconds(self) -> int:
        return self.get_int("LIVE_DETECTION_INTERVAL_SECONDS", 60)

    @property
    def user_monitoring_timeout_hours(self) -> int:
        return self.get_int("USER_MONITORING_TIMEOUT_HOURS", 24)

    # HTTP & API Configuration
    @property
    def enable_proxy(self) -> bool:
        return self.get_bool("ENABLE_PROXY", True)

    @property
    def http_request_timeout(self) -> int:
        return self.get_int("HTTP_REQUEST_TIMEOUT_SECONDS", 8)

    @property
    def http_connect_timeout(self) -> int:
        return self.get_int("HTTP_CONNECT_TIMEOUT_SECONDS", 5)

    @property
    def max_retries(self) -> int:
        return self.get_int("MAX_RETRIES", 3)

    @property
    def api_request_delay_ms(self) -> int:
        return self.get_int("API_REQUEST_DELAY_MS", 1000)

    @property
    def fallback_on_protocol_error(self) -> bool:
        return self.get_bool("FALLBACK_ON_PROTOCOL_ERROR", True)

    # TikTok API Configuration
    @property
    def tiktok_api_base_url(self) -> str:
        return self.get_str("TIKTOK_API_BASE_URL", "https://www.tiktok.com")

    @property
    def tiktok_live_base_url(self) -> str:
        return self.get_str("TIKTOK_LIVE_BASE_URL", "https://webcast.tiktok.com")

    @property
    def enable_room_id_cache(self) -> bool:
        return self.get_bool("ENABLE_ROOM_ID_CACHE", True)

    @property
    def room_id_cache_ttl_minutes(self) -> int:
        return self.get_int("ROOM_ID_CACHE_TTL_MINUTES", 30)

    @property
    def proxy_rotation_interval_minutes(self) -> int:
        return self.get_int("PROXY_ROTATION_INTERVAL_MINUTES", 5)

    @property
    def proxy_port_min(self) -> int:
        return self.get_int("PROXY_PORT_MIN", 10000)

    @property
    def proxy_port_max(self) -> int:
        return self.get_int("PROXY_PORT_MAX", 60000)

    @property
    def enable_fallback_api(self) -> bool:
        return self.get_bool("ENABLE_FALLBACK_API", True)

    @property
    def tiktoklive_api_key(self) -> str:
        return self.get_str("TIKTOKLIVE_API_KEY", "")

    @property
    def tiktoklive_session_id(self) -> str:
        return self.get_str("TIKTOKLIVE_SESSION_ID", "")

    # Redis Configuration
    @property
    def redis_url(self) -> str:
        return self.get_str("REDIS_URL", "redis://localhost:6379/0")

    @property
    def redis_max_connections(self) -> int:
        return self.get_int("REDIS_MAX_CONNECTIONS", 10)

    @property
    def redis_connection_timeout(self) -> int:
        return self.get_int("REDIS_CONNECTION_TIMEOUT", 5)

    # Database Configuration
    @property
    def database_url(self) -> str:
        return self.get_str("DATABASE_URL", "postgresql://postgres:password@localhost:5432/tiktok_bot")

    # Deterministic Mode Configuration (for consistent testing)
    @property
    def deterministic_recording(self) -> bool:
        return self.get_bool("DETERMINISTIC_RECORDING", False)

    @property
    def disable_random_delays(self) -> bool:
        return self.get_bool("DISABLE_RANDOM_DELAYS", False)

    @property
    def standardize_timeouts(self) -> bool:
        return self.get_bool("STANDARDIZE_TIMEOUTS", False)

    @property
    def consistent_retry_intervals(self) -> bool:
        return self.get_bool("CONSISTENT_RETRY_INTERVALS", False)

    @property
    def fixed_user_agent_index(self) -> int:
        return self.get_int("FIXED_USER_AGENT_INDEX", 0)

    # Logging Configuration
    @property
    def log_level(self) -> str:
        return self.get_str("LOG_LEVEL", "INFO")

    @property
    def enable_file_logging(self) -> bool:
        return self.get_bool("ENABLE_FILE_LOGGING", True)

    @property
    def log_rotation_days(self) -> int:
        return self.get_int("LOG_ROTATION_DAYS", 7)

# Global configuration instance
config = EnvConfig()