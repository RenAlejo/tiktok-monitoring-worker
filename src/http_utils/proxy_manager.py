# proxy_manager.py - Simplified for monitoring worker
import random
import time
import threading
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
from utils.logger_manager import logger
import requests
from config.env_config import config

class ProxyStatus(Enum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    ERROR = "error"
    TESTING = "testing"

@dataclass
class ProxyInfo:
    host: str
    port: int
    username: str = None
    password: str = None
    status: ProxyStatus = ProxyStatus.ACTIVE
    last_used: float = 0
    error_count: int = 0
    success_count: int = 0
    response_time: float = 0

class ProxyManager:
    """
    Gestor global de proxies residenciales con rotación inteligente para monitoring worker
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
        self.proxies: List[ProxyInfo] = []
        self.current_proxy_index = 0
        self.rotation_lock = threading.Lock()
        self.test_timeout = config.http_connect_timeout
        self.max_error_count = 5
        self.rotation_interval = config.proxy_rotation_interval_minutes * 60  # Convert to seconds
        self.last_rotation = time.time()

        # Cargar proxies desde configuración
        self._load_proxies()

    def _load_proxies(self):
        """Carga la lista de proxies desde configuración"""
        try:
            from utils.utils import read_proxy_config
            proxy_config = read_proxy_config()

            for proxy_data in proxy_config.get('proxies', []):
                proxy = ProxyInfo(
                    host=proxy_data['host'],
                    port=proxy_data['port'],
                    username=proxy_data.get('username'),
                    password=proxy_data.get('password')
                )
                self.proxies.append(proxy)

            logger.info(f"Loaded {len(self.proxies)} proxies")

        except Exception as e:
            logger.error(f"Error loading proxy configuration: {e}")
            # Configuración de emergencia
            self._load_fallback_proxies()

    def _load_fallback_proxies(self):
        """Configuración de respaldo de proxies"""
        fallback_proxies = [
            {"host": "gate.decodo.com", "port": 10100, "username": "spf3yb9j9d", "password": "imFbRD8jqq2ip3V=1m"},
            {"host": "gate.decodo.com", "port": 10099, "username": "spf3yb9j9d", "password": "imFbRD8jqq2ip3V=1m"},
        ]

        for proxy_data in fallback_proxies:
            proxy = ProxyInfo(**proxy_data)
            self.proxies.append(proxy)

    def get_current_proxy(self) -> Optional[ProxyInfo]:
        """
        Obtiene el proxy actual.

        Modos:
        - Rotación automática (USE_SINGLE_ROTATING_PROXY=True): Usa configuración .env directa
        - Multi-puerto (False): Usa sistema antiguo con puertos aleatorios
        """
        with self.rotation_lock:
            current_time = time.time()

            # MODO 1: Proxy con rotación automática (nuevo)
            if config.use_single_rotating_proxy:
                # Validar configuración
                if not config.proxy_host or not config.proxy_port:
                    logger.error("❌ USE_SINGLE_ROTATING_PROXY enabled but PROXY_HOST/PROXY_PORT not configured")
                    return None

                logger.debug(f"🔀 Using single rotating proxy: {config.proxy_host}:{config.proxy_port}")

                return ProxyInfo(
                    host=config.proxy_host,
                    port=config.proxy_port,
                    username=config.proxy_username if config.proxy_username else None,
                    password=config.proxy_password if config.proxy_password else None,
                    status=ProxyStatus.ACTIVE,
                    last_used=current_time,
                    error_count=0,
                    success_count=0,
                    response_time=0
                )

            # MODO 2: Multi-puerto con rotación manual (antiguo - mantener compatibilidad)
            else:
                if not self.proxies:
                    logger.warning("No proxies configured for multi-port mode")
                    return None

                # Auto-rotate based on time interval
                time_since_rotation = current_time - self.last_rotation

                if time_since_rotation >= self.rotation_interval:
                    elapsed_minutes = time_since_rotation / 60
                    logger.info(f"⏰ Auto-rotating proxy after {elapsed_minutes:.1f} minutes")

                    # Rotate inline (already have lock, don't call _rotate_to_next)
                    old_index = self.current_proxy_index
                    self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
                    logger.info(f"🔄 Proxy rotated: #{old_index} → #{self.current_proxy_index} (total: {len(self.proxies)})")

                    self.last_rotation = current_time

                if self.current_proxy_index >= len(self.proxies):
                    self.current_proxy_index = 0

                # Get base proxy configuration
                base_proxy = self.proxies[self.current_proxy_index]

                # Generate random port for IP rotation using configured range
                random_port = random.randint(config.proxy_port_min, config.proxy_port_max)
                ip_pool_size = config.proxy_port_max - config.proxy_port_min
                logger.info(f"🔀 Generated random proxy port: {base_proxy.host}:{random_port} (from {ip_pool_size} IP pool)")

                # Create new ProxyInfo with random port
                return ProxyInfo(
                    host=base_proxy.host,
                    port=random_port,
                    username=base_proxy.username,
                    password=base_proxy.password,
                    status=base_proxy.status,
                    last_used=current_time,
                    error_count=base_proxy.error_count,
                    success_count=base_proxy.success_count,
                    response_time=base_proxy.response_time
                )

    def mark_proxy_success(self, proxy: ProxyInfo, response_time: float):
        """Marca un proxy como exitoso"""
        proxy.success_count += 1
        proxy.response_time = response_time
        proxy.status = ProxyStatus.ACTIVE

    def mark_proxy_error(self, proxy: ProxyInfo, error_reason: str):
        """Marca un proxy como con error"""
        proxy.error_count += 1
        if proxy.error_count >= self.max_error_count:
            proxy.status = ProxyStatus.BLOCKED
        self._rotate_to_next()
        self.last_rotation = time.time()  # Reset timer after error rotation

    def _rotate_to_next(self):
        """Rota al siguiente proxy con logging"""
        with self.rotation_lock:
            old_index = self.current_proxy_index
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
            logger.info(f"🔄 Proxy rotated: #{old_index} → #{self.current_proxy_index} (total: {len(self.proxies)})")

    def format_proxy_for_requests(self, proxy: ProxyInfo) -> Dict[str, str]:
        """Formatea proxy para uso con requests"""
        if proxy.username and proxy.password:
            proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
        else:
            proxy_url = f"http://{proxy.host}:{proxy.port}"

        return {
            'http': proxy_url,
            'https': proxy_url
        }