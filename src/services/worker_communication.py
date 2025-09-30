"""
Inter-Worker Communication Service
Handles communication between monitoring workers and recording workers
"""
import json
import time
import threading
from typing import Dict, List, Optional, Callable, Any
from dataclasses import asdict
from utils.logger_manager import logger
from services.redis_queue_service import RedisQueueService
from services.monitoring_models import RecordingRequest, MonitoringSubscriber

class WorkerCommunicationService:
    """
    Servicio de comunicación entre workers
    """

    def __init__(self, worker_id: str, redis_service: RedisQueueService):
        self.worker_id = worker_id
        self.redis_service = redis_service
        self.message_handlers: Dict[str, Callable] = {}
        self.is_listening = False
        self.listener_thread = None

        # Register default message handlers
        self._register_default_handlers()

    def _register_default_handlers(self):
        """Registra handlers por defecto para mensajes"""
        self.register_handler("recording_completed", self._handle_recording_completed)
        self.register_handler("recording_failed", self._handle_recording_failed)
        self.register_handler("worker_status_request", self._handle_status_request)
        self.register_handler("monitoring_assignment_request", self._handle_assignment_request)

    def register_handler(self, message_type: str, handler: Callable):
        """Registra un handler para un tipo de mensaje"""
        self.message_handlers[message_type] = handler
        logger.debug(f"Registered handler for message type: {message_type}")

    def start_listening(self):
        """Inicia el listener de mensajes"""
        if self.is_listening:
            return

        self.is_listening = True
        self.listener_thread = threading.Thread(target=self._message_listener, daemon=True)
        self.listener_thread.start()
        logger.info(f"Started message listener for worker {self.worker_id}")

    def stop_listening(self):
        """Detiene el listener de mensajes"""
        if not self.is_listening:
            return

        self.is_listening = False
        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=5)
        logger.info(f"Stopped message listener for worker {self.worker_id}")

    def _message_listener(self):
        """Loop del listener de mensajes"""
        while self.is_listening:
            try:
                # Listen for messages on worker's command channel
                message = self.redis_service.pubsub.get_message(timeout=1)

                if message and message['type'] == 'message':
                    self._process_message(message)

            except Exception as e:
                logger.error(f"Error in message listener: {e}")
                time.sleep(1)

    def _process_message(self, message):
        """Procesa un mensaje recibido"""
        try:
            data = json.loads(message['data'])
            message_type = data.get('type')
            payload = data.get('payload', {})

            logger.debug(f"Received message type: {message_type}")

            if message_type in self.message_handlers:
                self.message_handlers[message_type](payload)
            else:
                logger.warning(f"No handler for message type: {message_type}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def send_message_to_worker(self, target_worker_id: str, message_type: str, payload: Dict):
        """Envía un mensaje a un worker específico"""
        try:
            message = {
                "type": message_type,
                "from_worker": self.worker_id,
                "timestamp": time.time(),
                "payload": payload
            }

            channel = f"worker_commands:{target_worker_id}"
            self.redis_service.redis_client.publish(channel, json.dumps(message))

            logger.debug(f"Sent message to {target_worker_id}: {message_type}")

        except Exception as e:
            logger.error(f"Failed to send message to {target_worker_id}: {e}")

    def broadcast_message(self, message_type: str, payload: Dict):
        """Envía un mensaje broadcast a todos los workers"""
        try:
            message = {
                "type": message_type,
                "from_worker": self.worker_id,
                "timestamp": time.time(),
                "payload": payload
            }

            self.redis_service.redis_client.publish(
                self.redis_service.BROADCAST_CHANNEL,
                json.dumps(message)
            )

            logger.debug(f"Broadcast message: {message_type}")

        except Exception as e:
            logger.error(f"Failed to broadcast message: {e}")

    def send_recording_request_with_callback(self, request: RecordingRequest,
                                           success_callback: Optional[Callable] = None,
                                           error_callback: Optional[Callable] = None):
        """
        Envía solicitud de grabación con callbacks para respuesta
        """
        try:
            # Store callbacks for this request
            if success_callback:
                self.register_handler(
                    f"recording_completed_{request.request_id}",
                    success_callback
                )

            if error_callback:
                self.register_handler(
                    f"recording_failed_{request.request_id}",
                    error_callback
                )

            # Send the recording request
            success = self.redis_service.send_recording_request(request)

            if not success:
                logger.error(f"Failed to send recording request: {request.request_id}")
                if error_callback:
                    error_callback({"error": "Failed to queue recording request"})

        except Exception as e:
            logger.error(f"Error sending recording request with callback: {e}")
            if error_callback:
                error_callback({"error": str(e)})

    def query_worker_status(self, target_worker_id: str) -> Optional[Dict]:
        """
        Consulta el estado de otro worker
        """
        try:
            # Send status request
            self.send_message_to_worker(
                target_worker_id,
                "worker_status_request",
                {"request_id": f"status_{int(time.time())}"}
            )

            # TODO: Implement response waiting mechanism
            # For now, just log the request
            logger.info(f"Status request sent to worker {target_worker_id}")

        except Exception as e:
            logger.error(f"Error querying worker status: {e}")
            return None

    def request_monitoring_assignment(self, username: str) -> bool:
        """
        Solicita asignación de monitoreo para un usuario
        """
        try:
            # Check if user is already assigned
            existing_worker = self.redis_service.check_user_monitoring_exists(username)

            if existing_worker:
                # Send assignment request to existing worker
                self.send_message_to_worker(
                    existing_worker,
                    "monitoring_assignment_request",
                    {
                        "username": username,
                        "requesting_worker": self.worker_id,
                        "action": "add_subscriber"
                    }
                )
                return True
            else:
                # No existing assignment, can proceed normally
                return False

        except Exception as e:
            logger.error(f"Error requesting monitoring assignment: {e}")
            return False

    # Default message handlers

    def _handle_recording_completed(self, payload: Dict):
        """Handler para grabación completada"""
        try:
            request_id = payload.get("request_id")
            username = payload.get("username")
            file_path = payload.get("file_path")

            logger.info(f"📹 Recording completed for {username}: {file_path}")

            # TODO: Notify subscribers about completion
            # This would involve sending notifications back to the bot

        except Exception as e:
            logger.error(f"Error handling recording completion: {e}")

    def _handle_recording_failed(self, payload: Dict):
        """Handler para grabación fallida"""
        try:
            request_id = payload.get("request_id")
            username = payload.get("username")
            error = payload.get("error")

            logger.error(f"❌ Recording failed for {username}: {error}")

            # TODO: Retry mechanism or notify subscribers about failure

        except Exception as e:
            logger.error(f"Error handling recording failure: {e}")

    def _handle_status_request(self, payload: Dict):
        """Handler para solicitud de estado"""
        try:
            requesting_worker = payload.get("from_worker")
            request_id = payload.get("request_id")

            # TODO: Get actual worker status and send response
            logger.debug(f"Status request from {requesting_worker}")

        except Exception as e:
            logger.error(f"Error handling status request: {e}")

    def _handle_assignment_request(self, payload: Dict):
        """Handler para solicitud de asignación de monitoreo"""
        try:
            username = payload.get("username")
            requesting_worker = payload.get("requesting_worker")
            action = payload.get("action")

            logger.info(f"Assignment request from {requesting_worker} for {username}: {action}")

            # TODO: Handle adding subscriber to existing monitoring job

        except Exception as e:
            logger.error(f"Error handling assignment request: {e}")

    def get_communication_stats(self) -> Dict:
        """Obtiene estadísticas de comunicación"""
        return {
            "worker_id": self.worker_id,
            "is_listening": self.is_listening,
            "registered_handlers": list(self.message_handlers.keys()),
            "listener_thread_alive": self.listener_thread.is_alive() if self.listener_thread else False
        }