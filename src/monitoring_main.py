#!/usr/bin/env python3
"""
TikTok Monitoring Worker - Main Entry Point
"""
import os
import sys
import time
import argparse
from typing import Optional

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))

from utils.logger_manager import logger
from config.env_config import config
from services.monitoring_worker import MonitoringWorker
from services.monitoring_models import MonitoringSubscriber, SubscriberType

def create_test_monitoring_job(worker: MonitoringWorker, username: str):
    """Crea un trabajo de monitoreo de prueba"""
    try:
        subscriber = MonitoringSubscriber(
            subscriber_id="test_bot_user",
            subscriber_type=SubscriberType.BOT_USER,
            chat_id=123456789,
            user_id=987654321,
            language="en"
        )

        success = worker.add_monitoring_job(username, subscriber)

        if success:
            logger.info(f"✅ Test monitoring job created for {username}")
        else:
            logger.warning(f"⚠️ Failed to create test monitoring job for {username}")

        return success

    except Exception as e:
        logger.error(f"❌ Error creating test job: {e}")
        return False

def run_worker_with_test_job(worker_id: Optional[str] = None, test_username: Optional[str] = None):
    """Ejecuta el worker con un trabajo de monitoreo de prueba"""
    worker = None

    try:
        # Create worker
        worker = MonitoringWorker(worker_id)

        logger.info("🚀 Starting TikTok Monitoring Worker")
        logger.info(f"📱 Worker ID: {worker.worker_id}")
        logger.info(f"⚙️ Max concurrent jobs: {config.max_concurrent_monitoring_jobs}")
        logger.info(f"🔄 Monitoring interval: {config.monitoring_interval_seconds}s")

        # Start worker
        worker.start()

        # Add test job if specified
        if test_username:
            logger.info(f"🧪 Adding test monitoring job for: {test_username}")
            time.sleep(2)  # Wait for worker to fully start
            create_test_monitoring_job(worker, test_username)

        # Keep worker running
        logger.info("✅ Monitoring worker is running. Press Ctrl+C to stop.")

        try:
            while worker.is_running:
                time.sleep(1)

                # Print status every 30 seconds
                if int(time.time()) % 30 == 0:
                    status = worker.get_worker_status()
                    logger.info(f"📊 Worker Status: {status['active_jobs']} jobs, "
                              f"{status['load_percentage']:.1f}% load, "
                              f"uptime: {status['uptime_seconds']:.0f}s")

        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")

    except Exception as e:
        logger.error(f"❌ Worker error: {e}")
        sys.exit(1)

    finally:
        if worker:
            logger.info("🔄 Shutting down worker...")
            worker.stop()
            logger.info("✅ Worker shutdown complete")

def run_interactive_mode():
    """Modo interactivo para gestionar el worker"""
    worker = None

    try:
        # Create worker
        worker = MonitoringWorker()
        worker.start()

        logger.info("🎮 Interactive Mode - TikTok Monitoring Worker")
        logger.info(f"📱 Worker ID: {worker.worker_id}")

        while True:
            print("\n" + "="*50)
            print("TikTok Monitoring Worker - Interactive Mode")
            print("="*50)
            print("1. Add monitoring job")
            print("2. Remove monitoring job")
            print("3. Show worker status")
            print("4. List active jobs")
            print("5. Show statistics")
            print("6. Exit")
            print("="*50)

            choice = input("Choose option (1-6): ").strip()

            if choice == "1":
                username = input("Enter TikTok username to monitor: ").strip()
                if username:
                    if username.startswith('@'):
                        username = username[1:]

                    subscriber = MonitoringSubscriber(
                        subscriber_id=f"interactive_user_{int(time.time())}",
                        subscriber_type=SubscriberType.BOT_USER,
                        chat_id=123456789,
                        user_id=987654321,
                        language="en"
                    )

                    success = worker.add_monitoring_job(username, subscriber)
                    if success:
                        print(f"✅ Started monitoring {username}")
                    else:
                        print(f"❌ Failed to start monitoring {username}")

            elif choice == "2":
                username = input("Enter username to stop monitoring: ").strip()
                if username:
                    if username.startswith('@'):
                        username = username[1:]

                    success = worker.remove_monitoring_job(
                        username,
                        f"interactive_user_{int(time.time())}",
                        SubscriberType.BOT_USER
                    )
                    if success:
                        print(f"✅ Stopped monitoring {username}")
                    else:
                        print(f"❌ Failed to stop monitoring {username}")

            elif choice == "3":
                status = worker.get_worker_status()
                print(f"\n📊 Worker Status:")
                print(f"   Status: {status['status']}")
                print(f"   Active Jobs: {status['active_jobs']}/{status['max_jobs']}")
                print(f"   Load: {status['load_percentage']:.1f}%")
                print(f"   Uptime: {status['uptime_seconds']:.0f} seconds")

            elif choice == "4":
                status = worker.get_worker_status()
                jobs = status.get('jobs_detail', [])

                if not jobs:
                    print("📭 No active monitoring jobs")
                else:
                    print(f"\n📋 Active Jobs ({len(jobs)}):")
                    for job in jobs:
                        live_status = "🔴 LIVE" if job['is_live'] else "🔵 Offline"
                        print(f"   • {job['username']}: {live_status} "
                              f"({job['subscribers']} subscribers, "
                              f"{job['total_checks']} checks, "
                              f"{job['live_detections']} live detections)")

            elif choice == "5":
                status = worker.get_worker_status()
                stats = status.get('statistics', {})

                print(f"\n📈 Statistics:")
                print(f"   Total Cycles: {stats.get('total_monitoring_cycles', 0)}")
                print(f"   Live Detections: {stats.get('total_live_detections', 0)}")
                print(f"   Recording Requests: {stats.get('total_recording_requests', 0)}")
                print(f"   Errors: {stats.get('total_errors', 0)}")
                print(f"   Last Cycle Duration: {stats.get('last_cycle_duration', 0):.2f}s")

            elif choice == "6":
                print("👋 Exiting...")
                break

            else:
                print("❌ Invalid option")

    except KeyboardInterrupt:
        print("\n🛑 Received shutdown signal")

    except Exception as e:
        logger.error(f"❌ Interactive mode error: {e}")

    finally:
        if worker:
            logger.info("🔄 Shutting down worker...")
            worker.stop()
            logger.info("✅ Worker shutdown complete")

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description="TikTok Monitoring Worker")
    parser.add_argument("--worker-id", help="Custom worker ID")
    parser.add_argument("--test-username", help="Username to monitor for testing")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")

    args = parser.parse_args()

    logger.info("🚀 TikTok Monitoring Worker Starting...")
    logger.info(f"📋 Configuration:")
    logger.info(f"   Redis URL: {config.redis_url}")
    logger.info(f"   Max Jobs: {config.max_concurrent_monitoring_jobs}")
    logger.info(f"   Monitoring Interval: {config.monitoring_interval_seconds}s")
    logger.info(f"   Live Detection Interval: {config.live_detection_interval_seconds}s")

    if args.interactive:
        run_interactive_mode()
    else:
        run_worker_with_test_job(args.worker_id, args.test_username)

if __name__ == "__main__":
    main()