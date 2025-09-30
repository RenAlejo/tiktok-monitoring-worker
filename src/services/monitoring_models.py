"""
Data models for TikTok Monitoring Worker
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from enum import Enum
import time

class MonitoringJobType(Enum):
    """Tipos de trabajos de monitoreo"""
    USER_MONITORING = "user_monitoring"
    LIVE_DETECTION = "live_detection"
    SUBSCRIBER_MANAGEMENT = "subscriber_management"

class MonitoringStatus(Enum):
    """Estados de monitoreo"""
    PENDING = "pending"
    ACTIVE = "active"
    LIVE_DETECTED = "live_detected"
    RECORDING_TRIGGERED = "recording_triggered"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"

class SubscriberType(Enum):
    """Tipos de suscriptores"""
    BOT_USER = "bot_user"
    INTERNAL_SYSTEM = "internal_system"

@dataclass
class MonitoringSubscriber:
    """Suscriptor a un monitoreo"""
    subscriber_id: str
    subscriber_type: SubscriberType
    chat_id: int
    user_id: int
    language: str = "en"
    subscribed_at: float = field(default_factory=time.time)
    is_active: bool = True

@dataclass
class MonitoringJob:
    """Trabajo de monitoreo de usuario"""
    job_id: str
    job_type: MonitoringJobType
    target_username: str
    status: MonitoringStatus

    # Worker assignment
    assigned_worker_id: Optional[str] = None

    # Timing
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    last_check_at: Optional[float] = None
    expires_at: Optional[float] = None

    # Subscribers
    subscribers: List[MonitoringSubscriber] = field(default_factory=list)

    # TikTok API data
    current_room_id: Optional[str] = None
    last_known_live_status: bool = False

    # Monitoring configuration
    monitoring_interval_seconds: int = 60
    live_check_interval_seconds: int = 30

    # Error handling
    error_count: int = 0
    last_error: Optional[str] = None

    # Statistics
    total_checks: int = 0
    total_live_detections: int = 0

    def add_subscriber(self, subscriber: MonitoringSubscriber):
        """Añade un suscriptor al monitoreo"""
        # Verificar si ya existe
        for existing in self.subscribers:
            if (existing.subscriber_id == subscriber.subscriber_id and
                existing.subscriber_type == subscriber.subscriber_type):
                existing.is_active = True
                return

        self.subscribers.append(subscriber)

    def remove_subscriber(self, subscriber_id: str, subscriber_type: SubscriberType):
        """Elimina un suscriptor del monitoreo"""
        for subscriber in self.subscribers:
            if (subscriber.subscriber_id == subscriber_id and
                subscriber.subscriber_type == subscriber_type):
                subscriber.is_active = False

    def get_active_subscribers(self) -> List[MonitoringSubscriber]:
        """Obtiene la lista de suscriptores activos"""
        return [s for s in self.subscribers if s.is_active]

    def has_active_subscribers(self) -> bool:
        """Verifica si tiene suscriptores activos"""
        return len(self.get_active_subscribers()) > 0

    def mark_live_detected(self, room_id: str):
        """Marca que se detectó que el usuario está live"""
        self.current_room_id = room_id
        self.last_known_live_status = True
        self.status = MonitoringStatus.LIVE_DETECTED
        self.total_live_detections += 1

    def mark_live_ended(self):
        """Marca que el usuario ya no está live"""
        self.current_room_id = None
        self.last_known_live_status = False
        if self.status == MonitoringStatus.LIVE_DETECTED:
            self.status = MonitoringStatus.ACTIVE

    def increment_error(self, error_message: str):
        """Incrementa el contador de errores"""
        self.error_count += 1
        self.last_error = error_message
        if self.error_count >= 5:
            self.status = MonitoringStatus.ERROR

    def reset_errors(self):
        """Resetea el contador de errores"""
        self.error_count = 0
        self.last_error = None
        if self.status == MonitoringStatus.ERROR:
            self.status = MonitoringStatus.ACTIVE

@dataclass
class MonitoringState:
    """Estado global del sistema de monitoreo"""
    worker_id: str

    # Jobs activos
    active_jobs: Dict[str, MonitoringJob] = field(default_factory=dict)  # username -> MonitoringJob

    # Statistics
    total_jobs_processed: int = 0
    total_live_detections: int = 0
    total_recording_requests: int = 0

    # Worker status
    started_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    is_healthy: bool = True

    def add_job(self, job: MonitoringJob):
        """Añade un trabajo de monitoreo"""
        self.active_jobs[job.target_username] = job
        job.assigned_worker_id = self.worker_id

    def remove_job(self, username: str):
        """Elimina un trabajo de monitoreo"""
        if username in self.active_jobs:
            del self.active_jobs[username]

    def get_job(self, username: str) -> Optional[MonitoringJob]:
        """Obtiene un trabajo de monitoreo"""
        return self.active_jobs.get(username)

    def has_job(self, username: str) -> bool:
        """Verifica si existe un trabajo para el usuario"""
        return username in self.active_jobs

    def get_active_job_count(self) -> int:
        """Obtiene el número de trabajos activos"""
        return len(self.active_jobs)

    def get_live_jobs(self) -> List[MonitoringJob]:
        """Obtiene trabajos con usuarios live detectados"""
        return [job for job in self.active_jobs.values()
                if job.status == MonitoringStatus.LIVE_DETECTED]

    def update_heartbeat(self):
        """Actualiza el heartbeat del worker"""
        self.last_heartbeat = time.time()
        self.is_healthy = True

@dataclass
class RecordingRequest:
    """Solicitud de grabación enviada a workers de grabación"""
    request_id: str
    target_username: str
    room_id: str

    # Subscriber information for notifications
    subscribers: List[MonitoringSubscriber]

    # Source monitoring job
    monitoring_job_id: str
    monitoring_worker_id: str

    # Request metadata
    created_at: float = field(default_factory=time.time)
    priority: int = 1  # 1=high, 2=normal, 3=low

    # Configuration
    recording_mode: str = "live"
    max_duration_minutes: Optional[int] = None