import time
import threading
from typing import Dict, Set, Optional
from dataclasses import dataclass
from utils.logger_manager import logger

@dataclass
class CacheEntry:
    timestamp: float
    is_valid_user: bool
    room_id: Optional[str] = None
    error_type: Optional[str] = None
    attempt_count: int = 1
    last_fallback_attempt: Optional[float] = None

class UserCache:
    """
    Cache para usuarios de TikTok para evitar requests repetitivos en monitoring worker
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return

        self._initialized = True
        self.cache: Dict[str, CacheEntry] = {}
        self.cache_lock = threading.Lock()
        self.cache_duration = 60 * 60  # 1 hora por defecto
        self.invalid_users: Set[str] = set()

        # Cargar configuración
        self._load_config()

    def _load_config(self):
        """Carga la configuración del cache"""
        try:
            from utils.utils import read_proxy_optimization_config
            config = read_proxy_optimization_config()
            optimization = config.get('proxy_optimization', {})

            if optimization.get('user_cache_enabled', True):
                self.cache_duration = optimization.get('user_cache_duration_minutes', 60) * 60
            else:
                self.cache_duration = 0  # Deshabilitado

            logger.info(f"User cache initialized with {self.cache_duration}s duration")
        except Exception as e:
            logger.error(f"Error loading user cache config: {e}")

    def is_enabled(self) -> bool:
        """Verifica si el cache está habilitado"""
        return self.cache_duration > 0

    def is_user_known_invalid(self, username: str) -> bool:
        """Verifica si un usuario está en la lista negra"""
        if not self.is_enabled():
            return False

        with self.cache_lock:
            current_time = time.time()

            if username in self.cache:
                entry = self.cache[username]

                # Si es un usuario válido, no está en lista negra
                if entry.is_valid_user:
                    return False

                # Verificar si el cache ha expirado
                if current_time - entry.timestamp < self.cache_duration:
                    return True
                else:
                    # Cache expirado, remover entrada
                    del self.cache[username]
                    self.invalid_users.discard(username)

        return False

    def cache_user_result(self, username: str, is_valid: bool, room_id: Optional[str] = None, error_type: Optional[str] = None):
        """Cachea el resultado de una consulta de usuario"""
        if not self.is_enabled():
            return

        with self.cache_lock:
            entry = CacheEntry(
                timestamp=time.time(),
                is_valid_user=is_valid,
                room_id=room_id,
                error_type=error_type
            )
            self.cache[username] = entry

            if not is_valid:
                self.invalid_users.add(username)

    def get_cached_room_id(self, username: str) -> Optional[str]:
        """Obtiene room_id cacheado si está disponible"""
        if not self.is_enabled():
            return None

        with self.cache_lock:
            if username in self.cache:
                entry = self.cache[username]
                current_time = time.time()

                if (current_time - entry.timestamp < self.cache_duration and
                    entry.is_valid_user and entry.room_id):
                    return entry.room_id

        return None

    def clear_cache(self):
        """Limpia todo el cache"""
        with self.cache_lock:
            self.cache.clear()
            self.invalid_users.clear()
            logger.info("User cache cleared")