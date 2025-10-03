"""
Base Monitoring Worker
Core worker class that handles monitoring jobs and coordinates with other workers
"""
import asyncio
import time
import threading
import signal
import sys
import json
from typing import Dict, List, Optional, Set
from utils.logger_manager import logger
from config.env_config import config
from services.redis_queue_service import RedisQueueService
from services.monitoring_models import (
    MonitoringJob, MonitoringState, MonitoringSubscriber, RecordingRequest,
    MonitoringJobType, MonitoringStatus, SubscriberType
)
from services.worker_command_handler import WorkerCommandHandler
from core.tiktok_api import TikTokAPI

class MonitoringWorker:
    """
    Worker de monitoreo que gestiona trabajos de monitoreo de usuarios TikTok
    """

    def __init__(self, worker_id: Optional[str] = None):
        self.worker_id = worker_id or config.worker_id
        self.state = MonitoringState(worker_id=self.worker_id)

        # Services
        self.redis_service = RedisQueueService(self.worker_id)
        self.tiktok_api = TikTokAPI()
        self.command_handler = WorkerCommandHandler(self)

        # Threading
        self.monitoring_threads: Dict[str, threading.Thread] = {}  # Per-job threads
        self.monitoring_threads_lock = threading.Lock()
        self.command_listener_thread = None
        self.is_running = False
        self.shutdown_event = threading.Event()

        # In-memory room_id cache for reducing TikTok API calls
        # Format: {username: (room_id, cached_timestamp)}
        self._room_id_cache: Dict[str, tuple[str, float]] = {}

        # Statistics
        self.stats = {
            "total_monitoring_cycles": 0,
            "total_live_detections": 0,
            "total_recording_requests": 0,
            "total_errors": 0,
            "last_cycle_duration": 0
        }

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        logger.info(f"Monitoring worker {self.worker_id} initialized")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, starting graceful shutdown...")
        self.stop()

    def start(self):
        """Inicia el worker de monitoreo"""
        if self.is_running:
            logger.warning("Worker already running")
            return

        try:
            logger.info(f"Starting monitoring worker {self.worker_id}")

            # Start Redis services
            self.redis_service.start_worker_services()


            # RECOVERY: Recuperar jobs desde Redis
            recovered_jobs = self.redis_service.recover_worker_jobs()
            reactivated_count = 0

            for job in recovered_jobs:
                # Reactivar jobs pausados si cumplen condiciones
                if job.status == MonitoringStatus.PAUSED:
                    # Condición 1: Tiene suscriptores activos
                    if not job.has_active_subscribers():
                        logger.info(f"⏸️ Keeping PAUSED job {job.target_username} (no active subscribers)")
                        self.state.add_job(job)
                        continue

                    # Condición 2: No hay grabación activa
                    if self.redis_service.check_active_recording(job.target_username):
                        logger.info(f"⏸️ Keeping PAUSED job {job.target_username} (active recording found)")
                        self.state.add_job(job)
                        continue

                    # Safe to reactivate
                    job.status = MonitoringStatus.ACTIVE
                    reactivated_count += 1
                    logger.info(f"🔄 Reactivated PAUSED job: {job.target_username}")

                self.state.add_job(job)
                # Persistir en Redis (refresh timestamps + status actualizado)
                self.redis_service.store_monitoring_job(job)

            if recovered_jobs:
                logger.info(f"🔄 Recovered {len(recovered_jobs)} monitoring jobs from previous session ({reactivated_count} reactivated from PAUSED)")

            # Start independent monitoring threads for each recovered job
            self.is_running = True
            with self.monitoring_threads_lock:
                for job in recovered_jobs:
                    monitor_thread = threading.Thread(
                        target=self._job_monitoring_loop,
                        args=(job,),
                        daemon=True,
                        name=f"monitor_{job.target_username}"
                    )
                    self.monitoring_threads[job.target_username] = monitor_thread
                    monitor_thread.start()
                    logger.info(f"🧵 Started independent thread for recovered job: {job.target_username}")

            # Start command listener
            self.command_listener_thread = threading.Thread(target=self._command_listener_loop, daemon=True)
            self.command_listener_thread.start()

            # Update state
            self.state.update_heartbeat()

            logger.info(f"Monitoring worker {self.worker_id} started successfully")

        except Exception as e:
            logger.error(f"Failed to start worker: {e}")
            self.stop()
            raise

    def stop(self):
        """Detiene el worker de monitoreo"""
        if not self.is_running:
            return

        logger.info(f"Stopping monitoring worker {self.worker_id}")

        # Signal shutdown
        self.is_running = False
        self.shutdown_event.set()

        # Wait for all monitoring threads to finish
        with self.monitoring_threads_lock:
            active_threads = list(self.monitoring_threads.items())

        if active_threads:
            logger.info(f"Waiting for {len(active_threads)} monitoring threads to stop...")
            for username, thread in active_threads:
                if thread.is_alive():
                    logger.debug(f"Waiting for thread {username}...")
                    thread.join(timeout=10)
                    if thread.is_alive():
                        logger.warning(f"⚠️  Thread {username} did not stop gracefully")

        # Wait for command listener thread
        if self.command_listener_thread and self.command_listener_thread.is_alive():
            self.command_listener_thread.join(timeout=5)

        # Stop Redis services
        self.redis_service.stop_worker_services()

        # Close TikTok API
        if self.tiktok_api:
            self.tiktok_api.close()

        logger.info(f"Monitoring worker {self.worker_id} stopped")

    def add_monitoring_job(self, target_username: str, subscriber: MonitoringSubscriber) -> bool:
        """
        Añade un trabajo de monitoreo para un usuario
        """
        try:
            # Check if user is already being monitored by another worker
            existing_worker = self.redis_service.check_user_monitoring_exists(target_username)

            if existing_worker and existing_worker != self.worker_id:
                logger.info(f"User {target_username} already monitored by worker {existing_worker}")
                # TODO: Add subscriber to existing monitoring job via Redis
                return False

            # Check worker capacity
            if self.state.get_active_job_count() >= config.max_concurrent_monitoring_jobs:
                logger.warning(f"Worker at capacity ({config.max_concurrent_monitoring_jobs} jobs)")
                return False

            # Try to assign monitoring to this worker
            if not self.redis_service.assign_monitoring_job(target_username):
                logger.info(f"Failed to assign monitoring for {target_username} - already assigned")
                return False

            # Check if we already have this job locally
            if self.state.has_job(target_username):
                existing_job = self.state.get_job(target_username)
                existing_job.add_subscriber(subscriber)
                logger.info(f"Added subscriber to existing monitoring job for {target_username}")
            else:
                # Create new monitoring job
                job = MonitoringJob(
                    job_id=f"monitoring_{target_username}_{int(time.time())}",
                    job_type=MonitoringJobType.USER_MONITORING,
                    target_username=target_username,
                    status=MonitoringStatus.PENDING,
                    monitoring_interval_seconds=config.monitoring_interval_seconds,
                    live_check_interval_seconds=config.live_detection_interval_seconds
                )

                job.add_subscriber(subscriber)
                job.started_at = time.time()
                job.status = MonitoringStatus.ACTIVE

                # Add to local state
                self.state.add_job(job)

                # Store in Redis
                self.redis_service.store_monitoring_job(job)

                # Start independent monitoring thread for this job
                with self.monitoring_threads_lock:
                    monitor_thread = threading.Thread(
                        target=self._job_monitoring_loop,
                        args=(job,),
                        daemon=True,
                        name=f"monitor_{target_username}"
                    )
                    self.monitoring_threads[target_username] = monitor_thread
                    monitor_thread.start()
                    logger.info(f"🧵 Started independent thread for {target_username}")

                logger.info(f"Created new monitoring job for {target_username}")

            # Update job count in Redis
            self.redis_service.update_worker_job_count(self.state.get_active_job_count())

            return True

        except Exception as e:
            logger.error(f"Failed to add monitoring job for {target_username}: {e}")
            return False

    def remove_monitoring_job(self, target_username: str, subscriber_id: str, subscriber_type: SubscriberType) -> bool:
        """
        Elimina un suscriptor de un trabajo de monitoreo
        """
        try:
            job = self.state.get_job(target_username)
            if not job:
                logger.warning(f"No monitoring job found for {target_username}")
                return False

            # Remove subscriber
            job.remove_subscriber(subscriber_id, subscriber_type)

            # If no active subscribers, remove entire job
            if not job.has_active_subscribers():
                logger.info(f"No active subscribers, removing monitoring job for {target_username}")

                # Remove from local state (this will cause thread to stop on next iteration)
                self.state.remove_job(target_username)

                # Remove from Redis
                self.redis_service.remove_monitoring_job(target_username)

                # Stop monitoring thread gracefully
                with self.monitoring_threads_lock:
                    thread = self.monitoring_threads.get(target_username)
                    if thread and thread.is_alive():
                        logger.info(f"🛑 Waiting for monitoring thread to stop for {target_username}")
                        # Thread will stop on its own when it detects job is no longer in state
                        thread.join(timeout=10)
                        if thread.is_alive():
                            logger.warning(f"⚠️  Monitoring thread for {target_username} did not stop gracefully")

                    # Remove from thread dict
                    if target_username in self.monitoring_threads:
                        del self.monitoring_threads[target_username]
                        logger.info(f"🗑️  Removed thread for {target_username}")
            else:
                # Update job in Redis
                self.redis_service.store_monitoring_job(job)

            # Update job count
            self.redis_service.update_worker_job_count(self.state.get_active_job_count())

            return True

        except Exception as e:
            logger.error(f"Failed to remove monitoring job for {target_username}: {e}")
            return False

    def _command_listener_loop(self):
        """Loop para escuchar comandos desde el bot"""
        logger.info("Starting command listener loop")

        while self.is_running and not self.shutdown_event.is_set():
            try:
                # Listen for commands from Redis pub/sub
                message = self.redis_service.pubsub.get_message(timeout=1)

                if message and message['type'] == 'message':
                    self._process_command_message(message)

            except Exception as e:
                logger.error(f"Error in command listener loop: {e}")
                self.shutdown_event.wait(timeout=1)

        logger.info("Command listener loop ended")

    def _job_monitoring_loop(self, job: MonitoringJob):
        """
        Loop de monitoreo independiente para un único job.
        Cada job tiene su propio hilo que valida cada live_check_interval_seconds.
        """
        logger.info(f"🧵 Starting independent monitoring thread for {job.target_username}")

        while self.is_running and not self.shutdown_event.is_set():
            try:
                # Check if job still exists in state (could be removed)
                if not self.state.has_job(job.target_username):
                    logger.info(f"🛑 Job {job.target_username} no longer in state, stopping thread")
                    break

                # Get fresh job reference from state
                current_job = self.state.get_job(job.target_username)
                if not current_job:
                    break

                # SKIP paused jobs (waiting for recording to complete)
                if current_job.status == MonitoringStatus.PAUSED:
                    logger.debug(f"⏸️  Job {job.target_username} is PAUSED, waiting...")
                    self.shutdown_event.wait(timeout=10)  # Check again in 10s
                    continue

                # Process monitoring job (check live status, send notifications, etc.)
                self._process_monitoring_job(current_job)

                # Update heartbeat periodically
                self.state.update_heartbeat()

                # Wait for next check interval (or until shutdown)
                self.shutdown_event.wait(timeout=current_job.live_check_interval_seconds)

            except Exception as e:
                logger.error(f"Error in monitoring loop for {job.target_username}: {e}")
                # Wait a bit before retrying
                self.shutdown_event.wait(timeout=5)

        logger.info(f"🛑 Monitoring thread ended for {job.target_username}")

    def _process_command_message(self, message):
        """Procesa mensaje de comando recibido"""
        try:
            # Parse command data
            command_data = json.loads(message['data'])

            # Process command through handler
            response = self.command_handler.handle_command(command_data)

            # Send response if needed (for now just log)
            if response:
                logger.debug(f"Command processed: {command_data.get('type')} -> {response.get('success', False)}")

        except Exception as e:
            logger.error(f"Error processing command message: {e}")

    def _process_monitoring_job(self, job: MonitoringJob):
        """Procesa un trabajo de monitoreo individual"""
        try:
            # SKIP paused jobs (waiting for recording to complete)
            if job.status == MonitoringStatus.PAUSED:
                return

            current_time = time.time()

            # Check if it's time to check this job
            if (job.last_check_at and
                current_time - job.last_check_at < job.live_check_interval_seconds):
                return

            job.last_check_at = current_time
            job.total_checks += 1

            # Check if user is live
            is_live, room_id = self._check_user_live_status(job.target_username)

            if is_live and room_id:
                # User is live!
                if not job.last_known_live_status:
                    # Newly detected live - invalidate cache to get fresh room_id next time
                    logger.info(f"LIVE DETECTED: {job.target_username} (room: {room_id})")
                    self._invalidate_room_id_cache(job.target_username)

                    job.mark_live_detected(room_id)

                    # Send live detection notification to bot
                    self.command_handler.send_live_detection_notification(
                        job.target_username,
                        room_id,
                        job.get_active_subscribers()
                    )

                    # Send recording request
                    self._send_recording_request(job, room_id)

                    # PAUSE monitoring while recording is in progress
                    job.status = MonitoringStatus.PAUSED
                    logger.info(f"⏸️  Monitoring PAUSED for {job.target_username} - recording in progress")

                    # Update statistics
                    self.stats["total_live_detections"] += 1
                    self.stats["total_recording_requests"] += 1
                    self.state.total_live_detections += 1
                    self.state.total_recording_requests += 1

                # Update job in Redis
                self.redis_service.store_monitoring_job(job)

            else:
                # User is not live
                if job.last_known_live_status:
                    # Live ended - invalidate cache to get fresh room_id next time
                    logger.info(f"LIVE ENDED: {job.target_username}")
                    self._invalidate_room_id_cache(job.target_username)
                    job.mark_live_ended()
                    self.redis_service.store_monitoring_job(job)

            # Reset errors on successful check
            job.reset_errors()

        except Exception as e:
            logger.error(f"Error processing monitoring job for {job.target_username}: {e}")
            job.increment_error(str(e))

            # Remove job if too many errors
            if job.error_count >= 5:
                logger.error(f"Too many errors for {job.target_username}, removing job")
                self.state.remove_job(job.target_username)
                self.redis_service.remove_monitoring_job(job.target_username)

    def _get_cached_room_id(self, username: str) -> Optional[str]:
        """
        Obtiene room_id del cache o lo obtiene de TikTok API si expiró
        Cache TTL configurable via ROOM_ID_CACHE_TTL_MINUTES (default: 30 min)
        """
        cache_ttl_seconds = config.room_id_cache_ttl_minutes * 60

        # Check if we have cached room_id
        if username in self._room_id_cache:
            room_id, cached_at = self._room_id_cache[username]
            age_seconds = time.time() - cached_at

            if age_seconds < cache_ttl_seconds:
                logger.debug(f"✓ Using cached room_id for {username} (age: {int(age_seconds)}s / {config.room_id_cache_ttl_minutes}min)")
                return room_id
            else:
                logger.info(f"⏰ Cache expired for {username} (age: {int(age_seconds)}s), fetching fresh room_id")

        # Cache miss or expired - fetch from TikTok API
        room_id = self.tiktok_api._get_room_id_from_api(username)

        if room_id:
            self._room_id_cache[username] = (room_id, time.time())
            logger.info(f"💾 Cached room_id for {username}: {room_id} (TTL: {config.room_id_cache_ttl_minutes}min)")

        return room_id

    def _invalidate_room_id_cache(self, username: str):
        """
        Invalida el cache de room_id para un usuario
        Útil cuando cambia el estado de live (offline→online o online→offline)
        """
        if username in self._room_id_cache:
            del self._room_id_cache[username]
            logger.debug(f"🗑️  Invalidated room_id cache for {username}")

    def _check_user_live_status(self, username: str) -> tuple[bool, Optional[str]]:
        """
        Verifica si un usuario está live
        Usa cache de room_id para reducir llamadas a TikTok API
        Returns: (is_live, room_id)
        """
        try:
            logger.debug(f"🔍 Checking live status for {username}...")

            # PASO 1: Obtener room_id (desde cache o API)
            validation_room_id = self._get_cached_room_id(username)

            if not validation_room_id:
                logger.info(f"❌ No room_id found for {username} - User is OFFLINE")
                return False, None

            # PASO 2: Verificar si el room está realmente vivo (siempre verifica)
            is_alive = self.tiktok_api.is_room_alive(validation_room_id)

            if not is_alive:
                logger.info(f"❌ Room {validation_room_id} is NOT alive - {username} is OFFLINE")
                return False, None

            # Usuario REALMENTE está live
            logger.info(f"✅ Room {validation_room_id} is ALIVE - {username} is LIVE")
            return True, validation_room_id

        except Exception as e:
            logger.error(f"Error checking live status for {username}: {e}")
            return False, None

    def _send_recording_request(self, job: MonitoringJob, room_id: str):
        """Envía solicitud de grabación a workers de grabación"""
        try:
            request = RecordingRequest(
                request_id=f"rec_{job.target_username}_{int(time.time())}",
                target_username=job.target_username,
                room_id=room_id,
                subscribers=job.get_active_subscribers(),
                monitoring_job_id=job.job_id,
                monitoring_worker_id=self.worker_id,
                priority=1  # High priority for live recordings
            )

            success = self.redis_service.send_recording_request(request)

            if success:
                job.status = MonitoringStatus.RECORDING_TRIGGERED
                logger.info(f"Recording request sent for {job.target_username}")

                # Send recording status notification
                self.command_handler.send_recording_status_notification(
                    job.target_username,
                    "recording_started",
                    job.get_active_subscribers(),
                    {"room_id": room_id, "request_id": request.request_id}
                )
            else:
                logger.error(f"Failed to send recording request for {job.target_username}")

                # Send failure notification
                self.command_handler.send_recording_status_notification(
                    job.target_username,
                    "recording_failed",
                    job.get_active_subscribers(),
                    {"room_id": room_id, "error": "Failed to queue recording request"}
                )

        except Exception as e:
            logger.error(f"Error sending recording request for {job.target_username}: {e}")

    def get_worker_status(self) -> Dict:
        """Obtiene el estado completo del worker"""
        try:
            redis_stats = self.redis_service.get_worker_stats()

            return {
                "worker_id": self.worker_id,
                "status": "running" if self.is_running else "stopped",
                "active_jobs": self.state.get_active_job_count(),
                "max_jobs": config.max_concurrent_monitoring_jobs,
                "load_percentage": (self.state.get_active_job_count() / config.max_concurrent_monitoring_jobs) * 100,
                "uptime_seconds": time.time() - self.state.started_at,
                "statistics": self.stats,
                "redis_info": redis_stats,
                "jobs_detail": [
                    {
                        "username": job.target_username,
                        "status": job.status.value,
                        "subscribers": len(job.get_active_subscribers()),
                        "total_checks": job.total_checks,
                        "live_detections": job.total_live_detections,
                        "last_check": job.last_check_at,
                        "is_live": job.last_known_live_status
                    }
                    for job in self.state.active_jobs.values()
                ]
            }

        except Exception as e:
            logger.error(f"Error getting worker status: {e}")
            return {"worker_id": self.worker_id, "status": "error", "error": str(e)}