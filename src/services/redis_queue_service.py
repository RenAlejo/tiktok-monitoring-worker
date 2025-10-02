"""
Redis Queue Service for TikTok Monitoring Worker
Handles all Redis communication, worker registration, and job distribution
"""
import json
import time
import redis
import threading
from typing import Dict, List, Optional, Any, Set
from dataclasses import asdict
from utils.logger_manager import logger
from config.env_config import config
from services.monitoring_models import (
    MonitoringJob, MonitoringState, RecordingRequest,
    MonitoringJobType, MonitoringStatus
)

class RedisQueueService:
    """
    Servicio centralizado para manejo de colas Redis en monitoring worker
    """

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.redis_client = None
        self.pubsub = None
        self.heartbeat_thread = None
        self.is_running = False
        self._lock = threading.Lock()

        # Redis key patterns
        self.WORKER_KEY = f"monitoring_worker:{self.worker_id}"
        self.WORKER_HEARTBEAT_KEY = f"monitoring_worker_heartbeat:{self.worker_id}"
        self.MONITORING_JOBS_KEY = f"monitoring_jobs:{self.worker_id}"

        # Global patterns
        self.ALL_WORKERS_PATTERN = "monitoring_worker:*"
        self.WORKER_DISCOVERY_KEY = "monitoring_workers_active"
        self.MONITORING_ASSIGNMENT_KEY = "monitoring_assignments"

        # Communication channels
        self.WORKER_COMMANDS_CHANNEL = f"worker_commands:{self.worker_id}"
        self.BROADCAST_CHANNEL = "monitoring_worker_broadcast"
        self.RECORDING_REQUEST_CHANNEL = "recording_requests"
        self.RECORDING_RESPONSE_CHANNEL = f"recording_responses:{self.worker_id}"

        # Initialize connection
        self._connect()

    def _connect(self):
        """Establece conexión con Redis"""
        try:
            self.redis_client = redis.from_url(
                config.redis_url,
                max_connections=config.redis_max_connections,
                socket_connect_timeout=config.redis_connection_timeout,
                decode_responses=True
            )

            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis for monitoring worker {self.worker_id}")

            # Setup pub/sub for commands
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(self.WORKER_COMMANDS_CHANNEL)
            self.pubsub.subscribe(self.BROADCAST_CHANNEL)

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def start_worker_services(self):
        """Inicia los servicios del worker"""
        with self._lock:
            if self.is_running:
                return

            self.is_running = True

            # Register worker
            self._register_worker()

            # Start heartbeat
            self._start_heartbeat()

            logger.info(f"Monitoring worker {self.worker_id} services started")

    def stop_worker_services(self):
        """Detiene los servicios del worker"""
        with self._lock:
            if not self.is_running:
                return

            self.is_running = False

            # Stop heartbeat thread
            if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                self.heartbeat_thread.join(timeout=2)

            # Unregister worker
            self._unregister_worker()

            # Close connections
            if self.pubsub:
                self.pubsub.close()

            logger.info(f"Monitoring worker {self.worker_id} services stopped")

    def _register_worker(self):
        """Registra el worker en Redis"""
        try:
            worker_info = {
                "worker_id": self.worker_id,
                "worker_type": "monitoring",
                "status": "active",
                "started_at": time.time(),
                "max_concurrent_jobs": config.max_concurrent_monitoring_jobs,
                "current_jobs": 0,
                "last_heartbeat": time.time()
            }

            # Store worker info
            self.redis_client.hset(
                self.WORKER_KEY,
                mapping=worker_info
            )

            # Add to active workers set
            self.redis_client.sadd(self.WORKER_DISCOVERY_KEY, self.worker_id)

            # Set expiry for worker key (cleanup if worker crashes)
            self.redis_client.expire(self.WORKER_KEY, config.worker_heartbeat_timeout_seconds * 2)

            logger.info(f"Worker {self.worker_id} registered successfully")

        except Exception as e:
            logger.error(f"Failed to register worker: {e}")
            raise

    def _unregister_worker(self):
        """Desregistra el worker de Redis"""
        try:
            # Remove from active workers
            self.redis_client.srem(self.WORKER_DISCOVERY_KEY, self.worker_id)

            # Delete worker info
            self.redis_client.delete(self.WORKER_KEY)
            self.redis_client.delete(self.WORKER_HEARTBEAT_KEY)
            self.redis_client.delete(self.MONITORING_JOBS_KEY)

            logger.info(f"Worker {self.worker_id} unregistered")

        except Exception as e:
            logger.error(f"Failed to unregister worker: {e}")

    def _start_heartbeat(self):
        """Inicia el thread de heartbeat"""
        def heartbeat_loop():
            while self.is_running:
                try:
                    self._send_heartbeat()
                    time.sleep(config.worker_heartbeat_interval_seconds)
                except Exception as e:
                    logger.error(f"Heartbeat error: {e}")
                    time.sleep(5)  # Retry after 5 seconds on error

        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def _send_heartbeat(self):
        """Envía heartbeat a Redis"""
        try:
            current_time = time.time()

            # Update heartbeat timestamp
            self.redis_client.hset(
                self.WORKER_KEY,
                "last_heartbeat", current_time
            )

            # Update dedicated heartbeat key
            self.redis_client.setex(
                self.WORKER_HEARTBEAT_KEY,
                config.worker_heartbeat_timeout_seconds,
                current_time
            )

            # Extend worker key expiry
            self.redis_client.expire(self.WORKER_KEY, config.worker_heartbeat_timeout_seconds * 2)

            
            # RENOVAR ASSIGNMENTS: Refrescar TTL de assignments para deteccion de workers caidos
            job_usernames = self.redis_client.smembers(self.MONITORING_JOBS_KEY)
            if job_usernames:
                with self.redis_client.pipeline() as pipe:
                    for username in job_usernames:
                        assignment_key = f"monitoring_assignment:{username}"
                        # Renovar solo si sigue asignado a este worker
                        current_owner = self.redis_client.get(assignment_key)
                        if current_owner == self.worker_id:
                            # Setear TTL corto para deteccion rapida de caida
                            pipe.expire(assignment_key, config.worker_heartbeat_timeout_seconds * 2)
                    pipe.execute()

        except Exception as e:
            logger.error(f"Failed to send heartbeat: {e}")

    def get_active_monitoring_workers(self) -> List[Dict]:
        """Obtiene lista de workers de monitoreo activos"""
        try:
            active_workers = []
            worker_ids = self.redis_client.smembers(self.WORKER_DISCOVERY_KEY)

            for worker_id in worker_ids:
                worker_key = f"monitoring_worker:{worker_id}"
                worker_info = self.redis_client.hgetall(worker_key)

                if worker_info:
                    # Check if worker is alive (heartbeat)
                    last_heartbeat = float(worker_info.get('last_heartbeat', 0))
                    current_time = time.time()

                    if current_time - last_heartbeat < config.worker_heartbeat_timeout_seconds:
                        active_workers.append(worker_info)
                    else:
                        # Remove dead worker
                        self.redis_client.srem(self.WORKER_DISCOVERY_KEY, worker_id)
                        self.redis_client.delete(worker_key)

            return active_workers

        except Exception as e:
            logger.error(f"Failed to get active workers: {e}")
            return []

    def check_user_monitoring_exists(self, username: str) -> Optional[str]:
        """
        Verifica si ya existe un worker monitoreando al usuario
        Retorna worker_id si existe, None si no
        """
        try:
            # Check assignment mapping
            assigned_worker = self.redis_client.hget(self.MONITORING_ASSIGNMENT_KEY, username)

            if assigned_worker:
                # Verify worker is still alive
                worker_key = f"monitoring_worker:{assigned_worker}"
                if self.redis_client.exists(worker_key):
                    return assigned_worker
                else:
                    # Clean up dead assignment
                    self.redis_client.hdel(self.MONITORING_ASSIGNMENT_KEY, username)

            return None

        except Exception as e:
            logger.error(f"Failed to check user monitoring: {e}")
            return None

    def check_active_recording(self, username: str) -> bool:
        """
        Verifica si existe una grabación activa para el usuario

        Busca claves recording:{username}:* con status=active
        Usado durante recovery para decidir si reactivar jobs PAUSED

        Args:
            username: TikTok username a verificar

        Returns:
            True si existe grabación activa, False si no
        """
        try:
            # Buscar todas las claves de grabación para este usuario
            recording_keys = self.redis_client.keys(f"recording:{username}:*")

            if not recording_keys:
                return False

            # Verificar si alguna grabación está activa
            for key in recording_keys:
                status = self.redis_client.hget(key, "status")
                if status == "active":
                    logger.debug(f"Found active recording for {username}: {key}")
                    return True

            return False

        except Exception as e:
            logger.error(f"Error checking active recording for {username}: {e}")
            # Fail-safe: asumir que hay grabación activa en caso de error
            # Esto previene duplicar grabaciones si hay problemas de conexión
            return True

    def assign_monitoring_job(self, username: str) -> bool:
        """
        Asigna el monitoreo de un usuario a este worker
        Retorna True si se asignó exitosamente, False si ya existe
        """
        try:
            # Use Redis SET with NX (only if not exists) for atomic assignment
            assignment_key = f"monitoring_assignment:{username}"

            # Set assignment without expiry - monitoreo 24/7/365
            # Expiry se maneja via heartbeat renewal (deteccion de workers caidos)
            success = self.redis_client.set(
                assignment_key,
                self.worker_id,
                nx=True  # Only set if key doesn't exist
            )

            if success:
                # Also add to global mapping for quick lookups
                self.redis_client.hset(self.MONITORING_ASSIGNMENT_KEY, username, self.worker_id)
                logger.info(f"Assigned monitoring for {username} to worker {self.worker_id}")
                return True
            else:
                logger.info(f"User {username} already being monitored by another worker")
                return False

        except Exception as e:
            logger.error(f"Failed to assign monitoring job: {e}")
            return False

    def release_monitoring_job(self, username: str):
        """Libera el monitoreo de un usuario"""
        try:
            assignment_key = f"monitoring_assignment:{username}"

            # Only remove if assigned to this worker
            current_assignment = self.redis_client.get(assignment_key)
            if current_assignment == self.worker_id:
                self.redis_client.delete(assignment_key)
                self.redis_client.hdel(self.MONITORING_ASSIGNMENT_KEY, username)
                logger.info(f"Released monitoring for {username} from worker {self.worker_id}")

        except Exception as e:
            logger.error(f"Failed to release monitoring job: {e}")

    def store_monitoring_job(self, job: MonitoringJob):
        """Almacena un trabajo de monitoreo en Redis"""
        try:
            job_data = asdict(job)

            # Convert enums to strings for JSON serialization
            job_data["job_type"] = job.job_type.value
            job_data["status"] = job.status.value

            # Convert subscriber enums
            for subscriber in job_data.get("subscribers", []):
                if "subscriber_type" in subscriber:
                    subscriber["subscriber_type"] = subscriber["subscriber_type"].value

            job_key = f"monitoring_job:{job.target_username}"

            # Store job data
            self.redis_client.hset(job_key, mapping={
                "job_data": json.dumps(job_data),
                "worker_id": self.worker_id,
                "updated_at": time.time()
            })

            # Set expiry
            # Add to worker's job list
            self.redis_client.sadd(self.MONITORING_JOBS_KEY, job.target_username)

        except Exception as e:
            logger.error(f"Failed to store monitoring job: {e}")

    def remove_monitoring_job(self, username: str):
        """Elimina un trabajo de monitoreo de Redis"""
        try:
            job_key = f"monitoring_job:{username}"

            # Remove job data
            self.redis_client.delete(job_key)

            # Remove from worker's job list
            self.redis_client.srem(self.MONITORING_JOBS_KEY, username)

            # Release assignment
            self.release_monitoring_job(username)

        except Exception as e:
            logger.error(f"Failed to remove monitoring job: {e}")


    def recover_worker_jobs(self) -> List[MonitoringJob]:
        """
        Recupera todos los jobs asignados a este worker desde Redis
        Se llama al iniciar el worker para restaurar estado tras reinicio

        Implementa self-healing: reconstruye el set monitoring_jobs:{worker_id}
        desde el hash global monitoring_assignments para recuperar jobs legacy
        creados antes de la implementación del recovery system.

        Returns:
            Lista de MonitoringJobs recuperados
        """
        try:
            logger.info(f"🔄 Starting job recovery for worker {self.worker_id}")

            # PASO 1: SELF-HEALING - Reconstruir set desde monitoring_assignments
            # Esto permite recuperar jobs legacy que no fueron agregados al set
            logger.info(f"🔍 Scanning monitoring_assignments for jobs assigned to {self.worker_id}")

            all_assignments = self.redis_client.hgetall(self.MONITORING_ASSIGNMENT_KEY)
            reconstructed_count = 0

            for username, assigned_worker in all_assignments.items():
                if assigned_worker == self.worker_id:
                    # Verificar que existe el job hash
                    job_key = f"monitoring_job:{username}"
                    if self.redis_client.exists(job_key):
                        # Reconstruir el set (idempotente - no duplica si ya existe)
                        self.redis_client.sadd(self.MONITORING_JOBS_KEY, username)
                        reconstructed_count += 1
                    else:
                        # Limpiar assignment huérfano (job no existe)
                        logger.warning(f"⚠️ Assignment found for {username} but job data missing, cleaning up")
                        self.redis_client.hdel(self.MONITORING_ASSIGNMENT_KEY, username)

            if reconstructed_count > 0:
                logger.info(f"🔧 Self-healing: Reconstructed {reconstructed_count} jobs from assignments")

            # PASO 2: Obtener lista completa de usernames (set reconstruido)
            job_usernames = self.redis_client.smembers(self.MONITORING_JOBS_KEY)

            if not job_usernames:
                logger.info(f"No jobs to recover for worker {self.worker_id}")
                return []

            logger.info(f"📋 Found {len(job_usernames)} total jobs to recover")

            recovered_jobs = []
            expired_jobs = []
            
            for username in job_usernames:
                try:
                    job_key = f"monitoring_job:{username}"
                    job_data_str = self.redis_client.hget(job_key, "job_data")
                    
                    if not job_data_str:
                        logger.warning(f"Job data not found for {username}, cleaning up")
                        expired_jobs.append(username)
                        continue
                    
                    # Deserializar job desde JSON
                    job_dict = json.loads(job_data_str)
                    
                    # Reconstruir MonitoringJob desde dict
                    job = self._deserialize_monitoring_job(job_dict)
                    
                    # Validar que tenga al menos 1 subscriber activo
                    if not job.has_active_subscribers():
                        logger.warning(f"Job {username} has no active subscribers, skipping recovery")
                        expired_jobs.append(username)
                        continue
                    
                    # Verificar assignment (debe estar asignado a este worker)
                    assigned_worker = self.redis_client.hget(self.MONITORING_ASSIGNMENT_KEY, username)
                    if assigned_worker != self.worker_id:
                        logger.warning(f"Job {username} assigned to {assigned_worker}, not {self.worker_id}, skipping")
                        self.redis_client.srem(self.MONITORING_JOBS_KEY, username)
                        continue
                    
                    recovered_jobs.append(job)
                    logger.info(f"✅ Recovered job: {username} ({len(job.get_active_subscribers())} active subscribers)")
                    
                except Exception as e:
                    logger.error(f"Failed to recover job for {username}: {e}")
                    expired_jobs.append(username)
                    continue
            
            # Limpiar jobs expirados
            if expired_jobs:
                for username in expired_jobs:
                    self.redis_client.srem(self.MONITORING_JOBS_KEY, username)
                logger.info(f"🧹 Cleaned {len(expired_jobs)} expired jobs")
            
            logger.info(f"🎉 Recovery complete: {len(recovered_jobs)} jobs recovered")
            return recovered_jobs
            
        except Exception as e:
            logger.error(f"Failed to recover worker jobs: {e}")
            return []

    def _deserialize_monitoring_job(self, job_dict: dict) -> MonitoringJob:
        """
        Deserializa un MonitoringJob desde dict JSON
        
        Args:
            job_dict: Diccionario con datos del job
            
        Returns:
            MonitoringJob reconstruido
        """
        from services.monitoring_models import MonitoringSubscriber, SubscriberType
        
        # Convertir enums desde strings
        job_dict["job_type"] = MonitoringJobType(job_dict["job_type"])
        job_dict["status"] = MonitoringStatus(job_dict["status"])
        
        # Deserializar subscribers
        subscribers = []
        for sub_dict in job_dict.get("subscribers", []):
            sub_dict["subscriber_type"] = SubscriberType(sub_dict["subscriber_type"])
            subscriber = MonitoringSubscriber(**sub_dict)
            subscribers.append(subscriber)
        
        job_dict["subscribers"] = subscribers
        
        # Crear MonitoringJob
        return MonitoringJob(**job_dict)

    def send_recording_request(self, request: RecordingRequest) -> bool:
        """
        Envía UNA solicitud de grabación con TODOS los subscribers en metadata
        (Similar al monolito: una grabación compartida por todos)
        """
        try:
            # Filtrar solo subscribers activos
            active_subscribers = [sub for sub in request.subscribers if sub.is_active]

            if not active_subscribers:
                logger.warning(f"No active subscribers found for recording request: {request.target_username}")
                return False

            # PRIMER subscriber es el usuario primario (quien inicia la grabación)
            primary_subscriber = active_subscribers[0]

            # DEMÁS subscribers van en metadata como additional_subscribers
            additional_subscribers = [
                {
                    "user_id": str(sub.user_id),
                    "chat_id": str(sub.chat_id),
                    "language": sub.language,
                    "subscriber_type": sub.subscriber_type.value
                }
                for sub in active_subscribers[1:]  # Skip first
            ]

            # Generate job_id para el usuario primario
            job_id = f"rec_{request.target_username}_{int(request.created_at)}_{primary_subscriber.subscriber_id}"

            # EXACT format as bot - ALL VALUES AS STRINGS
            job_data = {
                "job_id": job_id,
                "job_type": "recording_request",
                "username": request.target_username,
                "user_id": str(primary_subscriber.user_id),  # STRING - usuario primario
                "chat_id": str(primary_subscriber.chat_id),  # STRING - usuario primario
                "language": primary_subscriber.language,
                "mode": str(request.recording_mode),
                "priority": str(request.priority),  # STRING
                "created_at": str(request.created_at),  # STRING
                "assigned_worker": "",
                "status": "pending",
                "metadata": json.dumps({  # JSON STRING
                    "room_id": request.room_id,
                    "monitoring_job_id": request.monitoring_job_id,
                    "monitoring_worker_id": request.monitoring_worker_id,
                    "subscriber_type": primary_subscriber.subscriber_type.value,
                    "auto_triggered": True,
                    "max_duration_minutes": request.max_duration_minutes,
                    "additional_subscribers": additional_subscribers  # SUSCRIPTORES ADICIONALES
                })
            }

            # Use EXACT same keys as bot
            job_key = f"job:{job_id}"  # Worker expects "job:{job_id}"
            queue_key = f"job_queue:{request.priority}"

            # Use pipeline like bot does (atomic operation)
            pipe = self.redis_client.pipeline()

            # 1. Store job data with hset (like bot)
            pipe.hset(job_key, mapping=job_data)

            # 2. Add to priority queue with zadd (NOT lpush!)
            pipe.zadd(queue_key, {job_id: request.created_at})

            # 3. Execute all operations atomically
            pipe.execute()

            logger.info(f"Sent 1 recording job for {request.target_username} (room: {request.room_id}) with {len(active_subscribers)} total subscribers ({len(additional_subscribers)} additional)")

            return True

        except Exception as e:
            logger.error(f"Failed to send recording request: {e}")
            return False

    def update_worker_job_count(self, job_count: int):
        """Actualiza el contador de trabajos del worker"""
        try:
            self.redis_client.hset(self.WORKER_KEY, "current_jobs", job_count)
        except Exception as e:
            logger.error(f"Failed to update job count: {e}")

    def get_worker_stats(self) -> Dict:
        """Obtiene estadísticas del worker"""
        try:
            worker_info = self.redis_client.hgetall(self.WORKER_KEY)
            active_jobs = self.redis_client.scard(self.MONITORING_JOBS_KEY)

            return {
                "worker_id": self.worker_id,
                "status": worker_info.get("status", "unknown"),
                "active_jobs": active_jobs,
                "max_jobs": int(worker_info.get("max_concurrent_jobs", 0)),
                "load_percentage": (active_jobs / int(worker_info.get("max_concurrent_jobs", 1))) * 100,
                "started_at": float(worker_info.get("started_at", 0)),
                "last_heartbeat": float(worker_info.get("last_heartbeat", 0)),
                "uptime_seconds": time.time() - float(worker_info.get("started_at", time.time()))
            }

        except Exception as e:
            logger.error(f"Failed to get worker stats: {e}")
            return {"worker_id": self.worker_id, "status": "error"}

    def close(self):
        """Cierra las conexiones Redis"""
        self.stop_worker_services()
        if self.redis_client:
            self.redis_client.close()