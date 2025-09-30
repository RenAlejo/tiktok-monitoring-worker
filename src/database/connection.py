import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from utils.logger_manager import logger

class DatabaseManager:
    def __init__(self):
        self.database_url = self._build_database_url()
        self.engine = None
        self.SessionLocal = None
        self._initialize_database()

    def _build_database_url(self):
        """Construye la URL de conexión desde variables de entorno"""
        # Cargar variables de entorno
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            logger.warning("python-dotenv not installed. Using system environment variables.")

        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'tiktok_bot')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', '')

        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    def _initialize_database(self):
        """Inicializa la conexión a la base de datos"""
        try:
            self.engine = create_engine(
                self.database_url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False  # Cambiar a True para debug SQL
            )

            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            # Importar modelos aquí para evitar importaciones circulares
            from database.models import Base

            # Crear tablas si no existen
            Base.metadata.create_all(bind=self.engine)

            logger.info("Database connection initialized successfully for monitoring worker")

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    @contextmanager
    def get_session(self):
        """Context manager para obtener una sesión de base de datos"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def test_connection(self):
        """Prueba la conexión a la base de datos"""
        try:
            with self.get_session() as session:
                session.execute("SELECT 1")
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def close(self):
        """Cierra las conexiones de la base de datos"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")

# Instancia global del gestor de base de datos
db_manager = DatabaseManager()