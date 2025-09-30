"""
Bot Commands for TikTok Monitoring
Implements bot commands that interact with monitoring workers
"""
import re
import time
from typing import Dict, List, Optional, Tuple
from utils.logger_manager import logger
from services.bot_integration_service import BotIntegrationService
from services.monitoring_models import MonitoringSubscriber, SubscriberType

class BotCommands:
    """
    Comandos del bot para gestionar monitoreo de TikTok
    """

    def __init__(self, bot_integration_service: BotIntegrationService):
        self.integration = bot_integration_service

        # Command patterns
        self.USERNAME_PATTERN = re.compile(r'^@?([a-zA-Z0-9_\.]+)$')

    def validate_username(self, username: str) -> Tuple[bool, str]:
        """
        Valida un username de TikTok
        """
        if not username:
            return False, "Username cannot be empty"

        # Remove @ if present
        clean_username = username.lstrip('@')

        if not self.USERNAME_PATTERN.match(clean_username):
            return False, "Invalid username format. Use only letters, numbers, dots and underscores"

        if len(clean_username) < 2:
            return False, "Username must be at least 2 characters long"

        if len(clean_username) > 24:
            return False, "Username cannot be longer than 24 characters"

        return True, clean_username

    def cmd_start_monitoring(self, user_id: int, chat_id: int, username: str,
                           language: str = "en") -> Dict:
        """
        Comando: Iniciar monitoreo de usuario
        """
        try:
            # Validate username
            is_valid, result = self.validate_username(username)
            if not is_valid:
                return {
                    "success": False,
                    "error": result,
                    "error_type": "validation"
                }

            clean_username = result

            # Check if already monitored
            status = self.integration.get_monitoring_status(clean_username)
            if status.get("is_monitored"):
                # User is already monitored, add as subscriber
                return self._add_subscriber_to_existing_monitoring(
                    user_id, chat_id, clean_username, language, status
                )

            # Create new monitoring request
            subscriber = MonitoringSubscriber(
                subscriber_id=str(user_id),
                subscriber_type=SubscriberType.BOT_USER,
                chat_id=chat_id,
                user_id=user_id,
                language=language
            )

            result = self.integration.request_monitoring_start(clean_username, subscriber)

            if result["success"]:
                return {
                    "success": True,
                    "message": f"Started monitoring @{clean_username}",
                    "username": clean_username,
                    "assigned_worker": result.get("assigned_worker"),
                    "is_new_monitoring": True
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                    "error_type": "worker_error"
                }

        except Exception as e:
            logger.error(f"Error in start monitoring command: {e}")
            return {
                "success": False,
                "error": "Internal error occurred",
                "error_type": "internal"
            }

    def cmd_stop_monitoring(self, user_id: int, chat_id: int, username: str) -> Dict:
        """
        Comando: Parar monitoreo de usuario
        """
        try:
            # Validate username
            is_valid, result = self.validate_username(username)
            if not is_valid:
                return {
                    "success": False,
                    "error": result,
                    "error_type": "validation"
                }

            clean_username = result

            # Check if being monitored
            status = self.integration.get_monitoring_status(clean_username)
            if not status.get("is_monitored"):
                return {
                    "success": False,
                    "error": f"@{clean_username} is not being monitored",
                    "error_type": "not_monitored"
                }

            # Send stop request
            result = self.integration.request_monitoring_stop(
                clean_username,
                str(user_id),
                SubscriberType.BOT_USER
            )

            if result["success"]:
                return {
                    "success": True,
                    "message": f"Stopped monitoring @{clean_username}",
                    "username": clean_username,
                    "assigned_worker": result.get("assigned_worker")
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Failed to stop monitoring"),
                    "error_type": "worker_error"
                }

        except Exception as e:
            logger.error(f"Error in stop monitoring command: {e}")
            return {
                "success": False,
                "error": "Internal error occurred",
                "error_type": "internal"
            }

    def cmd_status(self, user_id: int, chat_id: int, username: Optional[str] = None) -> Dict:
        """
        Comando: Obtener estado de monitoreo
        """
        try:
            if username:
                # Get status for specific user
                is_valid, result = self.validate_username(username)
                if not is_valid:
                    return {
                        "success": False,
                        "error": result,
                        "error_type": "validation"
                    }

                clean_username = result
                status = self.integration.get_monitoring_status(clean_username)

                return {
                    "success": True,
                    "username": clean_username,
                    "status": status
                }
            else:
                # Get system status
                system_status = self.integration.get_system_status()

                return {
                    "success": True,
                    "system_status": system_status
                }

        except Exception as e:
            logger.error(f"Error in status command: {e}")
            return {
                "success": False,
                "error": "Failed to get status",
                "error_type": "internal"
            }

    def cmd_list_monitoring(self, user_id: int, chat_id: int) -> Dict:
        """
        Comando: Listar todos los monitoreos activos del usuario
        """
        try:
            # This would require tracking user subscriptions
            # For now, return system overview
            system_status = self.integration.get_system_status()

            active_workers = system_status.get("workers", {}).get("details", [])
            total_jobs = system_status.get("workers", {}).get("total_jobs", 0)

            return {
                "success": True,
                "total_monitorings": total_jobs,
                "active_workers": len(active_workers),
                "worker_details": active_workers,
                "message": f"System has {total_jobs} active monitorings across {len(active_workers)} workers"
            }

        except Exception as e:
            logger.error(f"Error in list monitoring command: {e}")
            return {
                "success": False,
                "error": "Failed to list monitorings",
                "error_type": "internal"
            }

    def cmd_worker_stats(self, user_id: int, chat_id: int) -> Dict:
        """
        Comando: Obtener estadísticas de workers (admin)
        """
        try:
            system_status = self.integration.get_system_status()

            return {
                "success": True,
                "system_status": system_status,
                "integration_stats": self.integration.get_integration_stats()
            }

        except Exception as e:
            logger.error(f"Error in worker stats command: {e}")
            return {
                "success": False,
                "error": "Failed to get worker stats",
                "error_type": "internal"
            }

    def _add_subscriber_to_existing_monitoring(self, user_id: int, chat_id: int,
                                             username: str, language: str,
                                             current_status: Dict) -> Dict:
        """
        Añade suscriptor a monitoreo existente
        """
        try:
            # Check if user is already subscribed
            # This would require checking existing subscribers
            # For now, assume we can add the subscriber

            assigned_worker = current_status.get("assigned_worker")

            if assigned_worker:
                # Send command to add subscriber
                command = {
                    "type": "add_subscriber",
                    "request_id": f"add_sub_{username}_{user_id}_{int(time.time())}",
                    "target_username": username,
                    "subscriber": {
                        "subscriber_id": str(user_id),
                        "subscriber_type": SubscriberType.BOT_USER.value,
                        "chat_id": chat_id,
                        "user_id": user_id,
                        "language": language
                    },
                    "timestamp": time.time()
                }

                success = self.integration._send_worker_command(assigned_worker, command)

                if success:
                    return {
                        "success": True,
                        "message": f"Added to existing monitoring for @{username}",
                        "username": username,
                        "assigned_worker": assigned_worker,
                        "is_new_monitoring": False,
                        "status": current_status.get("status", "active")
                    }
                else:
                    return {
                        "success": False,
                        "error": "Failed to add subscriber to existing monitoring",
                        "error_type": "worker_error"
                    }
            else:
                return {
                    "success": False,
                    "error": "Monitoring exists but no worker assigned",
                    "error_type": "worker_error"
                }

        except Exception as e:
            logger.error(f"Error adding subscriber to existing monitoring: {e}")
            return {
                "success": False,
                "error": "Failed to add to existing monitoring",
                "error_type": "internal"
            }

    def format_status_message(self, status_data: Dict, language: str = "en") -> str:
        """
        Formatea mensaje de estado para el bot
        """
        try:
            if not status_data.get("success"):
                return f"Error: {status_data.get('error', 'Unknown error')}"

            if "username" in status_data:
                # Single user status
                username = status_data["username"]
                status = status_data["status"]

                if status.get("is_monitored"):
                    live_status = "🔴 LIVE" if status.get("last_known_live_status") else "🔵 Offline"
                    checks = status.get("total_checks", 0)
                    detections = status.get("live_detections", 0)
                    worker = status.get("assigned_worker", "unknown")

                    return (f"📊 Status for @{username}:\n"
                           f"Status: {live_status}\n"
                           f"Worker: {worker}\n"
                           f"Checks: {checks}\n"
                           f"Live detections: {detections}")
                else:
                    return f"@{username} is not being monitored"

            elif "system_status" in status_data:
                # System status
                system = status_data["system_status"]
                workers = system.get("workers", {})

                return (f"🖥️ System Status:\n"
                       f"Active workers: {workers.get('total_active', 0)}\n"
                       f"Total jobs: {workers.get('total_jobs', 0)}\n"
                       f"System load: {workers.get('system_load_percentage', 0):.1f}%")

            else:
                return "Status information available"

        except Exception as e:
            logger.error(f"Error formatting status message: {e}")
            return "Error formatting status information"

    def format_success_message(self, result_data: Dict, language: str = "en") -> str:
        """
        Formatea mensaje de éxito para el bot
        """
        try:
            username = result_data.get("username", "")
            is_new = result_data.get("is_new_monitoring", True)

            if is_new:
                return f"✅ Started monitoring @{username}\nYou'll be notified when they go live!"
            else:
                return f"✅ Added to existing monitoring for @{username}\nYou'll be notified when they go live!"

        except Exception as e:
            logger.error(f"Error formatting success message: {e}")
            return "Operation completed successfully"

    def format_error_message(self, error_data: Dict, language: str = "en") -> str:
        """
        Formatea mensaje de error para el bot
        """
        try:
            error_type = error_data.get("error_type", "unknown")
            error_msg = error_data.get("error", "Unknown error")

            if error_type == "validation":
                return f"❌ Invalid username: {error_msg}"
            elif error_type == "not_monitored":
                return f"❌ {error_msg}"
            elif error_type == "worker_error":
                return f"⚠️ Worker error: {error_msg}"
            else:
                return f"❌ Error: {error_msg}"

        except Exception as e:
            logger.error(f"Error formatting error message: {e}")
            return "❌ An error occurred"