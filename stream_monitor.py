#!/usr/bin/env python3
"""
Stream Monitor - monitors Twitch streams and automatically starts recordings
"""
import time
import threading
import logging
from typing import Dict, Set
from datetime import datetime

logger = logging.getLogger(__name__)

class StreamMonitor:
    """
    Monitors Twitch streams and automatically starts recordings when streamers go live
    """
    
    def __init__(self, app, db, recording_manager):
        self.app = app
        self.db = db
        self.recording_manager = recording_manager
        self._lock = threading.RLock()
        self._monitor_threads: Dict[int, threading.Thread] = {}
        self._stop_events: Dict[int, threading.Event] = {}
        self._running = True
        
    def start_monitoring(self, streamer_id: int):
        """Start monitoring a specific streamer"""
        with self._lock:
            if streamer_id in self._monitor_threads:
                # Already monitoring
                return
            
            stop_event = threading.Event()
            thread = threading.Thread(
                target=self._monitor_streamer,
                args=(streamer_id, stop_event),
                name=f"monitor-{streamer_id}",
                daemon=True
            )
            
            self._monitor_threads[streamer_id] = thread
            self._stop_events[streamer_id] = stop_event
            thread.start()
            
            logger.info(f"Started monitoring streamer {streamer_id}")
    
    def stop_monitoring(self, streamer_id: int):
        """Stop monitoring a specific streamer"""
        with self._lock:
            stop_event = self._stop_events.get(streamer_id)
            if stop_event:
                stop_event.set()
            
            # Remove from tracking
            self._monitor_threads.pop(streamer_id, None)
            self._stop_events.pop(streamer_id, None)
            
            logger.info(f"Stopped monitoring streamer {streamer_id}")
    
    def start_monitoring_all_active(self):
        """Start monitoring all active streamers"""
        with self.app.app_context():
            try:
                from app import Streamer  # Import here to avoid circular imports
                active_streamers = Streamer.query.filter_by(is_active=True).all()
                
                for streamer in active_streamers:
                    self.start_monitoring(streamer.id)
                    
                logger.info(f"Started monitoring {len(active_streamers)} active streamers")
                
            except Exception as e:
                logger.exception(f"Error starting monitoring for all active streamers: {e}")
    
    def stop_all_monitoring(self):
        """Stop monitoring all streamers"""
        with self._lock:
            streamer_ids = list(self._monitor_threads.keys())
        
        for streamer_id in streamer_ids:
            self.stop_monitoring(streamer_id)
        
        logger.info("Stopped monitoring all streamers")
    
    def _monitor_streamer(self, streamer_id: int, stop_event: threading.Event):
        """Monitor a single streamer for live status"""
        logger.info(f"Starting monitor loop for streamer {streamer_id}")
        
        while not stop_event.is_set() and self._running:
            try:
                with self.app.app_context():
                    from app import Streamer, AppConfig, TwitchManager, StreamStatus  # Import here
                    
                    # Get streamer info
                    streamer = Streamer.query.get(streamer_id)
                    if not streamer or not streamer.is_active:
                        logger.info(f"Streamer {streamer_id} no longer active, stopping monitor")
                        break
                    
                    # Check if already recording
                    if self.recording_manager.is_streamer_recording(streamer_id):
                        # Already recording, wait and continue
                        if stop_event.wait(streamer.timer):
                            break
                        continue
                    
                    # Check Twitch status
                    try:
                        config = AppConfig(streamer)
                        twitch_manager = TwitchManager(config)
                        status, title = twitch_manager.check_user(streamer.twitch_name)
                        
                        logger.debug(f"Streamer {streamer.twitch_name} status: {status.name if status else 'None'}")
                        
                        if status == StreamStatus.ONLINE:
                            logger.info(f"Streamer {streamer.twitch_name} is live: {title}")
                            
                            # Update stream title in case we want to use it
                            if title and title.strip():
                                streamer.title = title.strip()
                                self.db.session.commit()
                            
                            # Start recording
                            recording_id = self.recording_manager.start_recording(streamer_id)
                            if recording_id:
                                logger.info(f"Started recording {recording_id} for {streamer.twitch_name}")
                            else:
                                logger.warning(f"Failed to start recording for {streamer.twitch_name}")
                        
                        elif status == StreamStatus.UNDESIRED_GAME:
                            logger.debug(f"Streamer {streamer.twitch_name} playing undesired game, skipping")
                        
                        elif status == StreamStatus.OFFLINE:
                            logger.debug(f"Streamer {streamer.twitch_name} is offline")
                        
                        elif status == StreamStatus.ERROR:
                            logger.warning(f"Error checking status for {streamer.twitch_name}")
                        
                    except Exception as e:
                        logger.exception(f"Error checking Twitch status for streamer {streamer_id}: {e}")
                
                # Wait for next check (with early exit on stop)
                if stop_event.wait(streamer.timer if 'streamer' in locals() else 360):
                    break
                    
            except Exception as e:
                logger.exception(f"Error in monitor loop for streamer {streamer_id}: {e}")
                # Wait a bit before retrying on error
                if stop_event.wait(60):
                    break
        
        logger.info(f"Monitor loop ended for streamer {streamer_id}")
        
        # Clean up
        with self._lock:
            self._monitor_threads.pop(streamer_id, None)
            self._stop_events.pop(streamer_id, None)
    
    def get_monitoring_status(self) -> Dict[int, bool]:
        """Get monitoring status for all streamers"""
        with self._lock:
            return {
                streamer_id: thread.is_alive() 
                for streamer_id, thread in self._monitor_threads.items()
            }
    
    def shutdown(self):
        """Shutdown the stream monitor"""
        logger.info("Shutting down stream monitor")
        self._running = False
        self.stop_all_monitoring()
        
        # Wait for threads to finish
        with self._lock:
            threads = list(self._monitor_threads.values())
        
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=5)


# Global stream monitor instance
stream_monitor = None

def init_stream_monitor(app, db, recording_manager):
    """Initialize the global stream monitor"""
    global stream_monitor
    stream_monitor = StreamMonitor(app, db, recording_manager)
    return stream_monitor

def get_stream_monitor():
    """Get the global stream monitor"""
    if stream_monitor is None:
        raise RuntimeError("Stream monitor not initialized")
    return stream_monitor
