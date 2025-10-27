"""
Worker Command Handler
Handles commands received from bot integration service
"""
import json
import time
from typing import Dict, Optional
from utils.logger_manager import logger
from services.monitoring_models import MonitoringSubscriber, SubscriberType, MonitoringStatus

class WorkerCommandHandler:
    """
    Manejador de comandos para monitoring workers
    """

    def __init__(self, worker):
        self.worker = worker  # Reference to MonitoringWorker
        self.command_handlers = {
            "start_monitoring": self._handle_start_monitoring,
            "stop_monitoring": self._handle_stop_monitoring,
            "add_subscriber": self._handle_add_subscriber,
            "remove_subscriber": self._handle_remove_subscriber,
            "get_status": self._handle_get_status,
            "ping": self._handle_ping,
            "recording_completed": self._handle_recording_completed,
            "recording_failed": self._handle_recording_failed
        }

    def handle_command(self, command_data: Dict) -> Optional[Dict]:
        """
        Maneja un comando recibido desde el bot
        """
        try:
            command_type = command_data.get("type")
            request_id = command_data.get("request_id")

            logger.debug(f"Handling command: {command_type} (request: {request_id})")

            if command_type in self.command_handlers:
                response = self.command_handlers[command_type](command_data)

                # Add request tracking info
                if response and request_id:
                    response["request_id"] = request_id
                    response["timestamp"] = time.time()
                    response["worker_id"] = self.worker.worker_id

                return response
            else:
                logger.warning(f"Unknown command type: {command_type}")
                return {
                    "success": False,
                    "error": f"Unknown command type: {command_type}",
                    "request_id": request_id
                }

        except Exception as e:
            logger.error(f"Error handling command: {e}")
            return {
                "success": False,
                "error": str(e),
                "request_id": command_data.get("request_id")
            }

    def _handle_start_monitoring(self, command_data: Dict) -> Dict:
        """
        Maneja comando de inicio de monitoreo
        """
        try:
            target_username = command_data.get("target_username")
            subscriber_data = command_data.get("subscriber", {})

            if not target_username:
                return {
                    "success": False,
                    "error": "Missing target_username"
                }

            # Convert subscriber data back to object
            subscriber = MonitoringSubscriber(
                subscriber_id=subscriber_data.get("subscriber_id"),
                subscriber_type=SubscriberType(subscriber_data.get("subscriber_type")),
                chat_id=subscriber_data.get("chat_id"),
                user_id=subscriber_data.get("user_id"),
                language=subscriber_data.get("language", "en"),
                subscribed_at=subscriber_data.get("subscribed_at", time.time())
            )

            # Add monitoring job
            success = self.worker.add_monitoring_job(target_username, subscriber)

            if success:
                # Send notification back to bot
                self._send_bot_notification("monitoring_started", {
                    "username": target_username,
                    "subscriber_id": subscriber.subscriber_id,
                    "worker_id": self.worker.worker_id
                })

                return {
                    "success": True,
                    "message": f"Started monitoring {target_username}",
                    "username": target_username
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to start monitoring (worker at capacity or user already assigned)"
                }

        except Exception as e:
            logger.error(f"Error in start monitoring command: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _handle_stop_monitoring(self, command_data: Dict) -> Dict:
        """
        Maneja comando de parar monitoreo
        """
        try:
            target_username = command_data.get("target_username")
            subscriber_id = command_data.get("subscriber_id")
            subscriber_type_str = command_data.get("subscriber_type")

            if not all([target_username, subscriber_id, subscriber_type_str]):
                return {
                    "success": False,
                    "error": "Missing required parameters"
                }

            subscriber_type = SubscriberType(subscriber_type_str)

            # Remove monitoring job
            success = self.worker.remove_monitoring_job(
                target_username,
                subscriber_id,
                subscriber_type
            )

            if success:
                # Send notification back to bot
                self._send_bot_notification("monitoring_stopped", {
                    "username": target_username,
                    "subscriber_id": subscriber_id,
                    "worker_id": self.worker.worker_id
                })

                return {
                    "success": True,
                    "message": f"Stopped monitoring {target_username}",
                    "username": target_username
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to stop monitoring (job not found)"
                }

        except Exception as e:
            logger.error(f"Error in stop monitoring command: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _handle_add_subscriber(self, command_data: Dict) -> Dict:
        """
        Maneja comando de añadir suscriptor
        """
        try:
            target_username = command_data.get("target_username")
            subscriber_data = command_data.get("subscriber", {})

            if not target_username:
                return {
                    "success": False,
                    "error": "Missing target_username"
                }

            # Get existing job
            job = self.worker.state.get_job(target_username)
            if not job:
                return {
                    "success": False,
                    "error": f"No monitoring job found for {target_username}"
                }

            # Create subscriber object
            subscriber = MonitoringSubscriber(
                subscriber_id=subscriber_data.get("subscriber_id"),
                subscriber_type=SubscriberType(subscriber_data.get("subscriber_type")),
                chat_id=subscriber_data.get("chat_id"),
                user_id=subscriber_data.get("user_id"),
                language=subscriber_data.get("language", "en"),
                subscribed_at=time.time()
            )

            # Add subscriber to existing job
            job.add_subscriber(subscriber)

            # Add to user monitoring index (mantiene SET sincronizado)
            user_id = subscriber.user_id
            index_key = f"user_monitorings:{user_id}"
            self.worker.redis_service.redis_client.sadd(index_key, target_username)
            logger.debug(f"Added {target_username} to monitoring index for user {user_id}")

            # If job was PAUSED, check if we should RESUME it
            was_paused = job.status == MonitoringStatus.PAUSED
            if was_paused:
                # Verificar si el job está pausado por una grabación activa
                # Si last_known_live_status=True, significa que está en live
                # y el job fue pausado por grabación activa → NO reactivar
                if job.last_known_live_status:
                    logger.info(f"⏸️  Job {target_username} remains PAUSED - recording in progress (added subscriber will receive copy)")
                    # NO cambiar status - debe seguir pausado durante la grabación
                else:
                    # Job pausado por falta de suscriptores (no por grabación)
                    job.status = MonitoringStatus.ACTIVE
                    logger.info(f"▶️  Monitoring RESUMED for {target_username} - new subscriber joined (no active recording)")

            # Update job in Redis
            self.worker.redis_service.store_monitoring_job(job)

            # Send notification back to bot
            notification_type = "monitoring_resumed" if was_paused else "subscriber_added"
            self._send_bot_notification(notification_type, {
                "username": target_username,
                "subscriber_id": subscriber.subscriber_id,
                "worker_id": self.worker.worker_id,
                "total_subscribers": len(job.get_active_subscribers()),
                "was_paused": was_paused
            })

            return {
                "success": True,
                "message": f"Added subscriber to {target_username} monitoring",
                "username": target_username,
                "total_subscribers": len(job.get_active_subscribers())
            }

        except Exception as e:
            logger.error(f"Error in add subscriber command: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _handle_remove_subscriber(self, command_data: Dict) -> Dict:
        """
        Maneja comando de remover suscriptor
        """
        try:
            target_username = command_data.get("target_username")
            subscriber_id = command_data.get("subscriber_id")
            subscriber_type_str = command_data.get("subscriber_type")

            if not all([target_username, subscriber_id, subscriber_type_str]):
                return {
                    "success": False,
                    "error": "Missing required parameters"
                }

            # Get existing job
            job = self.worker.state.get_job(target_username)
            if not job:
                return {
                    "success": False,
                    "error": f"No monitoring job found for {target_username}"
                }

            subscriber_type = SubscriberType(subscriber_type_str)

            # Remove subscriber
            job.remove_subscriber(subscriber_id, subscriber_type)

            # Remove from user monitoring index
            user_id = subscriber_id.replace("user_", "") if subscriber_id.startswith("user_") else subscriber_id
            index_key = f"user_monitorings:{user_id}"
            self.worker.redis_service.redis_client.srem(index_key, target_username)
            logger.debug(f"Removed {target_username} from monitoring index for user {user_id}")

            # If no active subscribers, PAUSE job instead of deleting it
            if not job.has_active_subscribers():
                # Set status to PAUSED - monitoring loop will skip this job
                job.status = MonitoringStatus.PAUSED

                # Keep job in state but mark as paused
                self.worker.redis_service.store_monitoring_job(job)

                logger.info(f"⏸️  Monitoring PAUSED for {target_username} - no active subscribers (will skip TikTok API calls)")

                self._send_bot_notification("monitoring_paused", {
                    "username": target_username,
                    "reason": "no_active_subscribers",
                    "worker_id": self.worker.worker_id
                })
            else:
                # Still have active subscribers - just update
                self.worker.redis_service.store_monitoring_job(job)

                logger.info(f"Subscriber {subscriber_id} removed from {target_username}, {len(job.get_active_subscribers())} subscribers remaining")

                self._send_bot_notification("subscriber_removed", {
                    "username": target_username,
                    "subscriber_id": subscriber_id,
                    "worker_id": self.worker.worker_id,
                    "total_subscribers": len(job.get_active_subscribers())
                })

            return {
                "success": True,
                "message": f"Removed subscriber from {target_username}",
                "username": target_username,
                "monitoring_continues": job.has_active_subscribers() if job else False
            }

        except Exception as e:
            logger.error(f"Error in remove subscriber command: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _handle_get_status(self, command_data: Dict) -> Dict:
        """
        Maneja comando de obtener estado
        """
        try:
            target_username = command_data.get("target_username")

            if target_username:
                # Get specific job status
                job = self.worker.state.get_job(target_username)
                if job:
                    return {
                        "success": True,
                        "username": target_username,
                        "status": job.status.value,
                        "subscribers": len(job.get_active_subscribers()),
                        "total_checks": job.total_checks,
                        "live_detections": job.total_live_detections,
                        "last_check": job.last_check_at,
                        "is_live": job.last_known_live_status,
                        "current_room_id": job.current_room_id
                    }
                else:
                    return {
                        "success": False,
                        "error": f"No monitoring job found for {target_username}"
                    }
            else:
                # Get worker status
                return {
                    "success": True,
                    "worker_status": self.worker.get_worker_status()
                }

        except Exception as e:
            logger.error(f"Error in get status command: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _handle_ping(self, command_data: Dict) -> Dict:
        """
        Maneja comando ping
        """
        return {
            "success": True,
            "message": "pong",
            "worker_id": self.worker.worker_id,
            "timestamp": time.time()
        }

    def _send_bot_notification(self, notification_type: str, payload: Dict):
        """
        Envía notificación al bot
        """
        try:
            notification = {
                "type": notification_type,
                "payload": payload,
                "worker_id": self.worker.worker_id,
                "timestamp": time.time()
            }

            # Send to bot notifications channel
            self.worker.redis_service.redis_client.publish(
                "bot_notifications",
                json.dumps(notification)
            )

            logger.debug(f"Sent bot notification: {notification_type}")

        except Exception as e:
            logger.error(f"Error sending bot notification: {e}")

    def send_live_detection_notification(self, username: str, room_id: str, subscribers: list):
        """
        Envía notificación de detección de live
        """
        try:
            self._send_bot_notification("live_detected", {
                "username": username,
                "room_id": room_id,
                "subscribers": [
                    {
                        "subscriber_id": sub.subscriber_id,
                        "chat_id": sub.chat_id,
                        "user_id": sub.user_id,
                        "language": sub.language
                    }
                    for sub in subscribers
                ],
                "detection_time": time.time()
            })

            logger.info(f"Sent live detection notification for {username} to {len(subscribers)} subscribers")

        except Exception as e:
            logger.error(f"Error sending live detection notification: {e}")

    def send_recording_status_notification(self, username: str, status: str,
                                         subscribers: list, additional_data: Dict = None):
        """
        Envía notificación de estado de grabación
        """
        try:
            payload = {
                "username": username,
                "recording_status": status,
                "subscribers": [
                    {
                        "subscriber_id": sub.subscriber_id,
                        "chat_id": sub.chat_id,
                        "user_id": sub.user_id,
                        "language": sub.language
                    }
                    for sub in subscribers
                ],
                "timestamp": time.time()
            }

            if additional_data:
                payload.update(additional_data)

            self._send_bot_notification("recording_status", payload)

            logger.info(f"Sent recording status notification for {username}: {status}")

        except Exception as e:
            logger.error(f"Error sending recording status notification: {e}")

    def _handle_recording_completed(self, command_data: Dict) -> Dict:
        """
        Maneja notificación de grabación completada desde recording worker
        Resume el monitoreo que estaba pausado
        """
        try:
            username = command_data.get("username")
            job_id = command_data.get("job_id")
            worker_id = command_data.get("worker_id")

            if not username:
                return {
                    "success": False,
                    "error": "Missing username"
                }

            logger.info(f"📹 Recording completed for {username} (by {worker_id})")

            # Buscar job de monitoreo
            job = self.worker.state.get_job(username)

            if job:
                # Reanudar monitoreo: cambiar de PAUSED a ACTIVE
                job.status = MonitoringStatus.ACTIVE
                job.last_known_live_status = False  # Reset para detectar siguiente live
                job.last_live_room_id = None

                # Guardar en Redis
                self.worker.redis_service.store_monitoring_job(job)

                logger.info(f"▶️  Monitoring RESUMED for {username} after recording completion")

                return {
                    "success": True,
                    "message": f"Monitoring resumed for {username}",
                    "username": username,
                    "status": "active"
                }
            else:
                logger.warning(f"No monitoring job found for {username}, cannot resume")
                return {
                    "success": False,
                    "error": f"No monitoring job found for {username}"
                }

        except Exception as e:
            logger.error(f"Error handling recording completion: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _handle_recording_failed(self, command_data: Dict) -> Dict:
        """
        Maneja notificación de grabación fallida desde recording worker
        Resume el monitoreo que estaba pausado
        """
        try:
            username = command_data.get("username")
            job_id = command_data.get("job_id")
            worker_id = command_data.get("worker_id")
            error = command_data.get("error", "Unknown error")

            if not username:
                return {
                    "success": False,
                    "error": "Missing username"
                }

            logger.warning(f"❌ Recording failed for {username} (by {worker_id}): {error}")

            # Buscar job de monitoreo
            job = self.worker.state.get_job(username)

            if job:
                # Reanudar monitoreo: cambiar de PAUSED a ACTIVE
                job.status = MonitoringStatus.ACTIVE
                job.last_known_live_status = False  # Reset para detectar siguiente live
                job.last_live_room_id = None

                # Guardar en Redis
                self.worker.redis_service.store_monitoring_job(job)

                logger.info(f"▶️  Monitoring RESUMED for {username} after recording failure")

                return {
                    "success": True,
                    "message": f"Monitoring resumed for {username} after failure",
                    "username": username,
                    "status": "active"
                }
            else:
                logger.warning(f"No monitoring job found for {username}, cannot resume")
                return {
                    "success": False,
                    "error": f"No monitoring job found for {username}"
                }

        except Exception as e:
            logger.error(f"Error handling recording failure: {e}")
            return {
                "success": False,
                "error": str(e)
            }