#!/usr/bin/env python3
"""
TikTok Monitoring Worker - Main Entry Point
Starts and manages a monitoring worker that handles 24/7 user monitoring
"""
import sys
import os
import argparse
import time
import threading
from utils.logger_manager import logger
from config.env_config import config
from services.monitoring_worker import MonitoringWorker

def setup_logging():
    """Setup logging configuration"""
    try:
        logger.info("Starting TikTok Monitoring Worker")
        logger.info(f"Worker ID: {config.worker_id}")
        logger.info(f"Redis URL: {config.redis_url}")
        logger.info(f"Max Concurrent Jobs: {config.max_concurrent_monitoring_jobs}")
    except Exception as e:
        print(f"Failed to setup logging: {e}")
        sys.exit(1)

def validate_config():
    """Validate required configuration"""
    required_configs = [
        ('worker_id', 'WORKER_ID'),
        ('redis_url', 'REDIS_URL')
    ]

    missing = []
    for attr, env_name in required_configs:
        if not hasattr(config, attr) or not getattr(config, attr):
            missing.append(env_name)

    if missing:
        logger.error(f"Missing required configuration: {', '.join(missing)}")
        logger.error("Please check your .env file or environment variables")
        return False

    return True

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='TikTok Monitoring Worker')
    parser.add_argument('--worker-id',
                       help='Worker ID (overrides WORKER_ID env var)')
    parser.add_argument('--max-jobs', type=int,
                       help='Maximum concurrent jobs (overrides MAX_CONCURRENT_MONITORING_JOBS)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')

    args = parser.parse_args()

    # Override config if specified
    if args.worker_id:
        config.worker_id = args.worker_id

    if args.max_jobs:
        config.max_concurrent_monitoring_jobs = args.max_jobs

    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)

    # Setup and validate
    setup_logging()

    if not validate_config():
        sys.exit(1)

    # Create and start worker
    worker = None
    try:
        logger.info("Initializing monitoring worker...")
        worker = MonitoringWorker(worker_id=config.worker_id)

        logger.info("Starting monitoring worker...")
        worker.start()

        logger.info("Monitoring worker started successfully!")
        logger.info("Press Ctrl+C to stop...")

        # Keep main thread alive
        try:
            while worker.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")

    except Exception as e:
        logger.error(f"Failed to start monitoring worker: {e}")
        return 1

    finally:
        if worker:
            try:
                logger.info("Stopping monitoring worker...")
                worker.stop()
                logger.info("Monitoring worker stopped")
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")

    return 0

if __name__ == "__main__":
    sys.exit(main())