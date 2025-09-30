from typing import Optional
from sqlalchemy import and_
from database.connection import db_manager
from database.models import RoomCache
from utils.logger_manager import logger
import time

class RoomCacheService:
    """Servicio para gestionar el cache de room_ids en PostgreSQL para monitoring worker"""

    def __init__(self):
        self.max_age_hours = 3  # Cache TTL 3 horas
        self.failed_attempts_threshold = 3
        self.failed_cooldown_minutes = 30

    def get_cached_room_id(self, username: str) -> Optional[str]:
        """Obtiene room_id del cache si es válido"""
        max_age_seconds = self.max_age_hours * 3600
        cutoff_time = int(time.time()) - max_age_seconds

        try:
            with db_manager.get_session() as session:
                cache_entry = session.query(RoomCache).filter(
                    and_(
                        RoomCache.username == username,
                        RoomCache.last_updated > cutoff_time
                    )
                ).first()

                if cache_entry is None:
                    return None

                # Verificar si ha fallado muchas veces recientemente
                if cache_entry.failed_attempts >= self.failed_attempts_threshold:
                    recent_fail_cutoff = int(time.time()) - (self.failed_cooldown_minutes * 60)
                    if cache_entry.last_failed > recent_fail_cutoff:
                        return None

                return cache_entry.room_id

        except Exception as e:
            logger.warning(f"Error reading cache for {username}: {e}")
            return None

    def cache_room_id(self, username: str, room_id: str, is_live: bool = True):
        """Guarda room_id en el cache"""
        try:
            with db_manager.get_session() as session:
                cache_entry = session.query(RoomCache).filter(
                    RoomCache.username == username
                ).first()

                if cache_entry:
                    cache_entry.room_id = room_id
                    cache_entry.last_updated = int(time.time())
                    cache_entry.is_live = is_live
                    cache_entry.failed_attempts = 0
                    cache_entry.last_failed = 0
                else:
                    cache_entry = RoomCache(
                        username=username,
                        room_id=room_id,
                        last_updated=int(time.time()),
                        is_live=is_live,
                        failed_attempts=0,
                        last_failed=0
                    )
                    session.add(cache_entry)

                session.commit()

        except Exception as e:
            logger.error(f"Error caching room_id for {username}: {e}")

    def mark_failed_attempt(self, username: str):
        """Marca un intento fallido para un usuario"""
        try:
            with db_manager.get_session() as session:
                cache_entry = session.query(RoomCache).filter(
                    RoomCache.username == username
                ).first()

                if cache_entry:
                    cache_entry.failed_attempts += 1
                    cache_entry.last_failed = int(time.time())
                else:
                    cache_entry = RoomCache(
                        username=username,
                        room_id="",
                        last_updated=int(time.time()),
                        is_live=False,
                        failed_attempts=1,
                        last_failed=int(time.time())
                    )
                    session.add(cache_entry)

                session.commit()

        except Exception as e:
            logger.error(f"Error marking failed attempt for {username}: {e}")

    def remove_cached_room_id(self, username: str):
        """Elimina room_id corrupto del cache"""
        try:
            with db_manager.get_session() as session:
                session.query(RoomCache).filter(
                    RoomCache.username == username
                ).delete()
                session.commit()
        except Exception as e:
            logger.error(f"Error removing cache for {username}: {e}")

    def cleanup_old_entries(self):
        """Limpia entradas antiguas del cache"""
        try:
            cutoff_time = int(time.time()) - (self.max_age_hours * 3600)
            with db_manager.get_session() as session:
                deleted = session.query(RoomCache).filter(
                    RoomCache.last_updated < cutoff_time
                ).delete()
                session.commit()
                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} old cache entries")
        except Exception as e:
            logger.error(f"Error cleaning up cache: {e}")