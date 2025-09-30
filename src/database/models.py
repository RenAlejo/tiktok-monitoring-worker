from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, Numeric, BigInteger, Index
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime, timedelta
from decimal import Decimal
import enum
import time

# Crear Base declarativa (versión moderna de SQLAlchemy)
Base = declarative_base()

class UserRole(enum.Enum):
    ADMIN = "admin"
    PREMIUM = "premium"
    FREE = "free"

class SubscriptionStatus(enum.Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    PENDING = "pending"

class PaymentStatus(enum.Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    telegram_id = Column(String(50), unique=True, nullable=False)
    username = Column(String(100))
    first_name = Column(String(100))
    last_name = Column(String(100))
    role = Column(String(20), default=UserRole.FREE.value)
    is_admin = Column(Boolean, default=False)
    language = Column(String(10), default='en')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_activity = Column(DateTime, default=datetime.utcnow)

    # Relaciones
    subscriptions = relationship("Subscription", back_populates="user")
    payments = relationship("Payment", back_populates="user")
    admin_config = relationship("AdminConfig", back_populates="user", uselist=False)

    def get_active_subscription(self):
        """Obtiene la suscripción activa del usuario"""
        for subscription in self.subscriptions:
            if subscription.status == SubscriptionStatus.ACTIVE.value and subscription.expires_at > datetime.utcnow():
                return subscription
        return None

    def is_premium(self) -> bool:
        """Verifica si el usuario tiene una suscripción premium activa"""
        return self.get_active_subscription() is not None

    def __repr__(self):
        return f"<User(telegram_id='{self.telegram_id}', role='{self.role}')>"

class Subscription(Base):
    __tablename__ = 'subscriptions'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    status = Column(String(20), default=SubscriptionStatus.PENDING.value)
    created_at = Column(DateTime, default=datetime.utcnow)
    activated_at = Column(DateTime)
    expires_at = Column(DateTime)
    payment_method = Column(String(50))
    subscription_type = Column(String(50), default='premium')

    # Relaciones
    user = relationship("User", back_populates="subscriptions")
    payments = relationship("Payment", back_populates="subscription")

    def is_active(self) -> bool:
        """Verifica si la suscripción está activa y no ha expirado"""
        return (self.status == SubscriptionStatus.ACTIVE.value and
                self.expires_at and self.expires_at > datetime.utcnow())

    def days_remaining(self) -> int:
        """Calcula los días restantes de la suscripción"""
        if not self.expires_at:
            return 0
        remaining = self.expires_at - datetime.utcnow()
        return max(0, remaining.days)

    def __repr__(self):
        return f"<Subscription(user_id={self.user_id}, status='{self.status}', expires='{self.expires_at}')>"

class Payment(Base):
    __tablename__ = 'payments'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    subscription_id = Column(Integer, ForeignKey('subscriptions.id'))
    amount = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(10), default='USD')
    payment_method = Column(String(50))
    transaction_id = Column(String(200))
    status = Column(String(20), default=PaymentStatus.PENDING.value)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    payment_metadata = Column(Text)  # JSON para datos adicionales

    # Relaciones
    user = relationship("User", back_populates="payments")
    subscription = relationship("Subscription", back_populates="payments")

    def __repr__(self):
        return f"<Payment(user_id={self.user_id}, amount={self.amount}, status='{self.status}')>"

class AdminConfig(Base):
    __tablename__ = 'admin_config'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    max_simultaneous_recordings = Column(Integer, default=5)
    can_access_all_recordings = Column(Boolean, default=False)
    can_manage_users = Column(Boolean, default=False)
    can_view_analytics = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relaciones
    user = relationship("User", back_populates="admin_config")

    def __repr__(self):
        return f"<AdminConfig(user_id={self.user_id}, max_recordings={self.max_simultaneous_recordings})>"

class RoomCache(Base):
    __tablename__ = 'room_cache'

    id = Column(Integer, primary_key=True)
    username = Column(String(100), nullable=False, unique=True)
    room_id = Column(String(50), nullable=False)
    last_updated = Column(BigInteger, nullable=False)  # Timestamp UNIX
    is_live = Column(Boolean, default=False)
    failed_attempts = Column(Integer, default=0)
    last_failed = Column(BigInteger, default=0)  # Timestamp UNIX

    # Índices para optimización
    __table_args__ = (
        Index('idx_username_updated', 'username', 'last_updated'),
        Index('idx_last_updated', 'last_updated'),
    )

    def __repr__(self):
        return f"<RoomCache(username='{self.username}', room_id='{self.room_id}', is_live={self.is_live})>"