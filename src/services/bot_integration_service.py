"""
Bot Integration Service
Handles communication between Telegram bot and monitoring workers
"""
import json
import time
import asyncio
import threading
from typing import Dict, List, Optional, Callable, Any
from dataclasses import asdict
from utils.logger_manager import logger
from config.env_config import config
from services.redis_queue_service import RedisQueueService
from services.monitoring_models import (
    MonitoringJob, MonitoringSubscriber, RecordingRequest,
    MonitoringJobType, MonitoringStatus, SubscriberType
)

class BotIntegrationService:
    """
    Servicio de integración entre bot de Telegram y monitoring workers
    """

    def __init__(self, service_id: str = "bot_integration"):
        self.service_id = service_id
        self.redis_service = RedisQueueService(service_id)

        # Channels for bot-worker communication
        self.BOT_COMMANDS_CHANNEL = "bot_commands"
        self.BOT_RESPONSES_CHANNEL = "bot_responses"
        self.BOT_NOTIFICATIONS_CHANNEL = "bot_notifications"

        # Request tracking
        self.pending_requests: Dict[str, Dict] = {}
        self.request_timeout_seconds = 30

        # Callbacks
        self.notification_callbacks: List[Callable] = []

        # Threading
        self.is_running = False
        self.listener_thread = None

    def start(self):
        """Inicia el servicio de integración"""
        try:
            logger.info(f"Starting bot integration service: {self.service_id}")

            # Start Redis services
            self.redis_service.start_worker_services()

            # Subscribe to notification channels
            self.redis_service.pubsub.subscribe(self.BOT_NOTIFICATIONS_CHANNEL)

            # Start listener thread
            self.is_running = True
            self.listener_thread = threading.Thread(target=self._notification_listener, daemon=True)
            self.listener_thread.start()

            logger.info(f"Bot integration service started successfully")

        except Exception as e:
            logger.error(f"Failed to start bot integration service: {e}")
            raise

    def stop(self):
        """Detiene el servicio de integración"""
        logger.info(f"Stopping bot integration service: {self.service_id}")

        self.is_running = False

        # Stop listener thread
        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=5)

        # Stop Redis services
        self.redis_service.stop_worker_services()

        logger.info("Bot integration service stopped")

    def _notification_listener(self):
        """Escucha notificaciones desde workers"""
        while self.is_running:
            try:
                message = self.redis_service.pubsub.get_message(timeout=1)

                if message and message['type'] == 'message':
                    self._process_notification(message)

            except Exception as e:
                logger.error(f"Error in notification listener: {e}")
                time.sleep(1)

    def _process_notification(self, message):
        """Procesa notificación desde workers"""
        try:
            data = json.loads(message['data'])
            notification_type = data.get('type')
            payload = data.get('payload', {})

            logger.debug(f"Received notification: {notification_type}")

            # Call registered callbacks
            for callback in self.notification_callbacks:
                try:
                    callback(notification_type, payload)
                except Exception as e:
                    logger.error(f"Error in notification callback: {e}")

        except Exception as e:
            logger.error(f"Error processing notification: {e}")

    def register_notification_callback(self, callback: Callable):
        """Registra callback para notificaciones"""
        self.notification_callbacks.append(callback)
        logger.debug(f"Registered notification callback: {callback.__name__}")

    def request_monitoring_start(self, username: str, subscriber: MonitoringSubscriber,
                                timeout: int = 30) -> Dict:
        """
        Solicita inicio de monitoreo para un usuario
        """
        try:
            request_id = f"start_monitoring_{username}_{int(time.time())}"

            # Find best worker for this monitoring job
            best_worker = self._find_best_monitoring_worker()

            if not best_worker:
                return {
                    "success": False,
                    "error": "No monitoring workers available",
                    "request_id": request_id
                }

            # Send command to specific worker
            command = {
                "type": "start_monitoring",
                "request_id": request_id,
                "target_username": username,
                "subscriber": asdict(subscriber),
                "timestamp": time.time()
            }

            # Convert enum to string for JSON serialization
            command["subscriber"]["subscriber_type"] = subscriber.subscriber_type.value

            success = self._send_worker_command(best_worker['worker_id'], command)

            if success:
                logger.info(f"Sent monitoring start request for {username} to worker {best_worker['worker_id']}")
                return {
                    "success": True,
                    "request_id": request_id,
                    "assigned_worker": best_worker['worker_id'],
                    "message": f"Monitoring request sent to worker {best_worker['worker_id']}"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to send command to worker",
                    "request_id": request_id
                }

        except Exception as e:
            logger.error(f"Error requesting monitoring start: {e}")
            return {
                "success": False,
                "error": str(e),
                "request_id": request_id
            }

    def request_monitoring_stop(self, username: str, subscriber_id: str,
                               subscriber_type: SubscriberType, timeout: int = 30) -> Dict:
        """
        Solicita parar monitoreo para un usuario
        """
        try:
            request_id = f"stop_monitoring_{username}_{int(time.time())}"

            # Find worker monitoring this user
            assigned_worker = self.redis_service.check_user_monitoring_exists(username)

            if not assigned_worker:
                return {
                    "success": False,
                    "error": f"No active monitoring found for {username}",
                    "request_id": request_id
                }

            # Send stop command
            command = {
                "type": "stop_monitoring",
                "request_id": request_id,
                "target_username": username,
                "subscriber_id": subscriber_id,
                "subscriber_type": subscriber_type.value,
                "timestamp": time.time()
            }

            success = self._send_worker_command(assigned_worker, command)

            if success:
                logger.info(f"Sent monitoring stop request for {username} to worker {assigned_worker}")
                return {
                    "success": True,
                    "request_id": request_id,
                    "assigned_worker": assigned_worker,
                    "message": f"Stop request sent to worker {assigned_worker}"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to send stop command to worker",
                    "request_id": request_id
                }

        except Exception as e:
            logger.error(f"Error requesting monitoring stop: {e}")
            return {
                "success": False,
                "error": str(e),
                "request_id": request_id
            }

    def get_monitoring_status(self, username: str) -> Dict:
        """
        Obtiene el estado de monitoreo de un usuario
        """
        try:
            # Check if user is being monitored
            assigned_worker = self.redis_service.check_user_monitoring_exists(username)

            if not assigned_worker:
                return {
                    "is_monitored": False,
                    "username": username,
                    "status": "not_monitored"
                }

            # Get job details from Redis
            job_key = f"monitoring_job:{username}"
            job_data = self.redis_service.redis_client.hget(job_key, "job_data")

            if job_data:
                job_info = json.loads(job_data)
                return {
                    "is_monitored": True,
                    "username": username,
                    "status": job_info.get("status", "unknown"),
                    "assigned_worker": assigned_worker,
                    "started_at": job_info.get("started_at"),
                    "last_check_at": job_info.get("last_check_at"),
                    "total_checks": job_info.get("total_checks", 0),
                    "live_detections": job_info.get("total_live_detections", 0),
                    "subscribers_count": len(job_info.get("subscribers", [])),
                    "last_known_live_status": job_info.get("last_known_live_status", False)
                }
            else:
                return {
                    "is_monitored": True,
                    "username": username,
                    "status": "assigned_but_no_data",
                    "assigned_worker": assigned_worker
                }

        except Exception as e:
            logger.error(f"Error getting monitoring status for {username}: {e}")
            return {
                "is_monitored": False,
                "username": username,
                "status": "error",
                "error": str(e)
            }

    def get_system_status(self) -> Dict:
        """
        Obtiene el estado general del sistema de monitoreo
        """
        try:
            # Get active workers
            active_workers = self.redis_service.get_active_monitoring_workers()

            total_jobs = 0
            total_capacity = 0
            worker_details = []

            for worker_info in active_workers:
                worker_id = worker_info.get('worker_id')
                current_jobs = int(worker_info.get('current_jobs', 0))
                max_jobs = int(worker_info.get('max_concurrent_jobs', 0))

                total_jobs += current_jobs
                total_capacity += max_jobs

                worker_details.append({
                    "worker_id": worker_id,
                    "current_jobs": current_jobs,
                    "max_jobs": max_jobs,
                    "load_percentage": (current_jobs / max_jobs * 100) if max_jobs > 0 else 0,
                    "status": worker_info.get('status', 'unknown'),
                    "uptime": time.time() - float(worker_info.get('started_at', time.time()))
                })

            # Get queue lengths
            queue_lengths = {}
            for priority in [1, 2, 3]:
                queue_key = f"recording_job_queue:{priority}"
                queue_lengths[f"priority_{priority}"] = self.redis_service.redis_client.llen(queue_key)

            return {
                "workers": {
                    "total_active": len(active_workers),
                    "total_jobs": total_jobs,
                    "total_capacity": total_capacity,
                    "system_load_percentage": (total_jobs / total_capacity * 100) if total_capacity > 0 else 0,
                    "details": worker_details
                },
                "queues": {
                    "recording_requests": queue_lengths
                },
                "system": {
                    "is_healthy": len(active_workers) > 0,
                    "timestamp": time.time()
                }
            }

        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                "workers": {"total_active": 0, "total_jobs": 0, "total_capacity": 0},
                "system": {"is_healthy": False, "error": str(e)}
            }

    def _find_best_monitoring_worker(self) -> Optional[Dict]:
        """
        Encuentra el mejor worker para asignar un nuevo monitoreo
        """
        try:
            active_workers = self.redis_service.get_active_monitoring_workers()

            if not active_workers:
                return None

            # Find worker with lowest load percentage
            best_worker = None
            lowest_load = float('inf')

            for worker in active_workers:
                current_jobs = int(worker.get('current_jobs', 0))
                max_jobs = int(worker.get('max_concurrent_jobs', 1))

                # Skip workers at capacity
                if current_jobs >= max_jobs:
                    continue

                load_percentage = (current_jobs / max_jobs) * 100

                if load_percentage < lowest_load:
                    lowest_load = load_percentage
                    best_worker = worker

            return best_worker

        except Exception as e:
            logger.error(f"Error finding best worker: {e}")
            return None

    def _send_worker_command(self, worker_id: str, command: Dict) -> bool:
        """
        Envía comando a un worker específico
        """
        try:
            channel = f"worker_commands:{worker_id}"
            message = json.dumps(command)

            result = self.redis_service.redis_client.publish(channel, message)

            # result > 0 means message was delivered to at least one subscriber
            return result > 0

        except Exception as e:
            logger.error(f"Error sending command to worker {worker_id}: {e}")
            return False

    def broadcast_command(self, command: Dict) -> int:
        """
        Envía comando broadcast a todos los workers
        """
        try:
            message = json.dumps(command)
            result = self.redis_service.redis_client.publish(
                self.redis_service.BROADCAST_CHANNEL,
                message
            )

            logger.debug(f"Broadcast command sent to {result} workers")
            return result

        except Exception as e:
            logger.error(f"Error broadcasting command: {e}")
            return 0

    def get_user_monitoring_history(self, username: str, limit: int = 10) -> List[Dict]:
        """
        Obtiene el historial de monitoreo de un usuario
        """
        try:
            # This would typically query a database or Redis logs
            # For now, return basic info from current state

            status = self.get_monitoring_status(username)

            if status.get("is_monitored"):
                return [{
                    "username": username,
                    "status": status.get("status"),
                    "started_at": status.get("started_at"),
                    "worker": status.get("assigned_worker"),
                    "checks": status.get("total_checks", 0),
                    "live_detections": status.get("live_detections", 0)
                }]
            else:
                return []

        except Exception as e:
            logger.error(f"Error getting monitoring history for {username}: {e}")
            return []

    def get_integration_stats(self) -> Dict:
        """
        Obtiene estadísticas del servicio de integración
        """
        return {
            "service_id": self.service_id,
            "is_running": self.is_running,
            "pending_requests": len(self.pending_requests),
            "notification_callbacks": len(self.notification_callbacks),
            "listener_active": self.listener_thread.is_alive() if self.listener_thread else False
        }