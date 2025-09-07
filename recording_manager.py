#!/usr/bin/env python3
"""
Simplified Recording Manager - replaces the complex recording logic in app.py
"""
import os
import sys
import time
import json
import threading
import subprocess
import logging
import shutil
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class RecordingState(Enum):
    STARTING = "starting"
    RECORDING = "recording" 
    STOPPING = "stopping"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class RecordingInfo:
    id: int
    streamer_id: int
    pid: Optional[int] = None
    process: Optional[subprocess.Popen] = None
    state: RecordingState = RecordingState.STARTING
    started_at: Optional[datetime] = None
    stop_event: Optional[threading.Event] = None
    thread: Optional[threading.Thread] = None
    filename: str = ""
    error_message: str = ""
    
    # NEW: Store database data to avoid queries in thread
    streamer_twitch_name: str = ""
    streamer_quality: str = "best"
    auth_data: dict = None

class RecordingManager:
    """
    Centralized recording manager that handles all recording operations
    with proper thread safety and state management.
    """
    
    def __init__(self, app, db):
        self.app = app
        self.db = db
        self._lock = threading.RLock()
        self._recordings: Dict[int, RecordingInfo] = {}  # recording_id -> RecordingInfo
        self._streamer_recordings: Dict[int, int] = {}  # streamer_id -> recording_id
        self._cleanup_thread = None
        self._running = True
        self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """Start background cleanup thread"""
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, 
            name="recording-cleanup", 
            daemon=True
        )
        self._cleanup_thread.start()
    
    def _cleanup_loop(self):
        """Background cleanup of stale recordings"""
        while self._running:
            try:
                self._cleanup_stale_recordings()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.exception(f"Cleanup loop error: {e}")
                time.sleep(5)
    
    def _cleanup_stale_recordings(self):
        """Remove recordings that are no longer active"""
        with self._lock:
            stale_ids = []
            for recording_id, info in self._recordings.items():
                if self._is_recording_stale(info):
                    stale_ids.append(recording_id)
            
            for recording_id in stale_ids:
                logger.info(f"Cleaning up stale recording {recording_id}")
                self._cleanup_recording_unsafe(recording_id)
    
    def _is_recording_stale(self, info: RecordingInfo) -> bool:
        """Check if a recording is stale and should be cleaned up"""
        # Process is dead
        if info.process and info.process.poll() is not None:
            return True
        
        # Thread is dead
        if info.thread and not info.thread.is_alive():
            return True
        
        # Recording is in terminal state
        if info.state in (RecordingState.COMPLETED, RecordingState.FAILED):
            return True
        
        return False
    
    def start_recording(self, streamer_id: int) -> Optional[int]:
        """
        Start recording for a streamer. Returns recording_id if successful, None if already recording.
        """
        with self._lock:
            # Check if already recording
            if streamer_id in self._streamer_recordings:
                existing_id = self._streamer_recordings[streamer_id]
                existing_info = self._recordings.get(existing_id)
                if existing_info and not self._is_recording_stale(existing_info):
                    logger.info(f"Streamer {streamer_id} already has active recording {existing_id}")
                    return None
                else:
                    # Clean up stale recording
                    self._cleanup_recording_unsafe(existing_id)
            
            # Create new recording in database
            with self.app.app_context():
                try:
                    from app import Streamer, Recording, TwitchAuth  # Import here to avoid circular imports
                    
                    # GET ALL DATABASE DATA FIRST (before starting thread)
                    streamer = Streamer.query.get(streamer_id)
                    if not streamer:
                        logger.error(f"Streamer {streamer_id} not found")
                        return None
                    
                    # Get auth data before starting thread
                    auth = TwitchAuth.query.first()
                    
                    # Create recording record
                    recording = Recording(
                        streamer_id=streamer_id,
                        filename=self._make_filename(streamer),
                        title="",  # Will be updated when stream info is fetched
                        status='recording',
                        started_at=datetime.utcnow()
                    )
                    self.db.session.add(recording)
                    self.db.session.commit()
                    
                    recording_id = recording.id
                    
                    # Create recording info with all the data needed
                    info = RecordingInfo(
                        id=recording_id,
                        streamer_id=streamer_id,
                        state=RecordingState.STARTING,
                        started_at=datetime.utcnow(),
                        stop_event=threading.Event(),
                        filename=recording.filename
                    )
                    
                    # Add the database data to the info object so thread doesn't need to query
                    info.streamer_twitch_name = streamer.twitch_name
                    info.streamer_quality = streamer.quality
                    info.auth_data = {
                        'oauth_token': auth.oauth_token if auth else None,
                        'client_id': auth.client_id if auth else None
                    }
                    
                    # Start recording thread
                    thread = threading.Thread(
                        target=self._recording_worker,
                        args=(info,),
                        name=f"recording-{recording_id}",
                        daemon=True
                    )
                    info.thread = thread
                    
                    # Register recording
                    self._recordings[recording_id] = info
                    self._streamer_recordings[streamer_id] = recording_id
                    
                    # Start the thread
                    thread.start()
                    
                    logger.info(f"Started recording {recording_id} for streamer {streamer_id}")
                    return recording_id
                    
                except Exception as e:
                    logger.exception(f"Error starting recording for streamer {streamer_id}: {e}")
                    self.db.session.rollback()
                    return None
    
    def stop_recording(self, recording_id: int) -> bool:
        """Stop a recording by recording_id"""
        with self._lock:
            info = self._recordings.get(recording_id)
            if not info:
                logger.warning(f"Recording {recording_id} not found")
                return False
            
            if info.state in (RecordingState.COMPLETED, RecordingState.FAILED):
                logger.info(f"Recording {recording_id} already stopped")
                return True
            
            logger.info(f"Stopping recording {recording_id}")
            info.state = RecordingState.STOPPING
            
            # Signal stop
            if info.stop_event:
                info.stop_event.set()
            
            # Terminate process if running
            if info.process and info.process.poll() is None:
                try:
                    info.process.terminate()
                    # Give it 10 seconds to terminate gracefully
                    try:
                        info.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"Process {info.process.pid} didn't terminate, killing it")
                        info.process.kill()
                except Exception as e:
                    logger.error(f"Error terminating process: {e}")
            
            return True
    
    def stop_recording_by_streamer(self, streamer_id: int) -> bool:
        """Stop recording for a streamer"""
        with self._lock:
            recording_id = self._streamer_recordings.get(streamer_id)
            if recording_id:
                return self.stop_recording(recording_id)
            return False
    
    def get_recording_info(self, recording_id: int) -> Optional[RecordingInfo]:
        """Get recording info by recording_id"""
        with self._lock:
            return self._recordings.get(recording_id)
    
    def get_active_recordings(self) -> Dict[int, RecordingInfo]:
        """Get all active recordings"""
        with self._lock:
            return {
                rid: info for rid, info in self._recordings.items()
                if info.state in (RecordingState.STARTING, RecordingState.RECORDING)
            }
    
    def is_streamer_recording(self, streamer_id: int) -> bool:
        """Check if a streamer is currently recording"""
        with self._lock:
            recording_id = self._streamer_recordings.get(streamer_id)
            if not recording_id:
                return False
            
            info = self._recordings.get(recording_id)
            return info and info.state in (RecordingState.STARTING, RecordingState.RECORDING)
    
    def _recording_worker(self, info: RecordingInfo):
        """Main recording worker thread"""
        try:
            with self.app.app_context():
                self._run_recording(info)
        except Exception as e:
            logger.exception(f"Recording worker error for {info.id}: {e}")
            info.state = RecordingState.FAILED
            info.error_message = str(e)
        finally:
            self._finalize_recording(info)
    
    def _run_recording(self, info: RecordingInfo):
        """Run the actual recording process (NO DATABASE ACCESS)"""
        from app import get_download_path
        
        try:  # ADD THIS TRY BLOCK
            # Use the data that was passed from start_recording (no database queries)
            streamer_twitch_name = info.streamer_twitch_name
            streamer_quality = info.streamer_quality
            auth_data = info.auth_data
            
            # Build output path
            download_path = get_download_path()
            os.makedirs(download_path, exist_ok=True)
            
            base_path = os.path.join(download_path, info.filename)
            ts_path = f"{base_path}.ts"
            
            # Build streamlink command (ultra-minimal version)
            cmd = [
                "streamlink",
                f"https://twitch.tv/{streamer_twitch_name}",
                streamer_quality,
                "-o", ts_path
            ]
            
            logger.info(f"Starting streamlink for recording {info.id}: {' '.join(cmd)}")  # ADD THIS LOG
            
            # Start process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=os.environ.copy()
            )
            info.process = process
            info.pid = process.pid
            info.state = RecordingState.RECORDING
            
            logger.info(f"Streamlink process started with PID {process.pid}")  # ADD THIS LOG
            
            # Start watchdog
            watchdog_thread = threading.Thread(
                target=self._watchdog_worker,
                args=(info,),
                name=f"watchdog-{info.id}",
                daemon=True
            )
            watchdog_thread.start()
            
            # Wait for process to complete
            return_code = process.wait()
            logger.info(f"Streamlink process finished with return code {return_code}")  # ADD THIS LOG
            
            if return_code == 0:
                info.state = RecordingState.COMPLETED
            else:
                info.state = RecordingState.FAILED
                info.error_message = f"Process exited with code {return_code}"
                
        except Exception as e:  # ADD THIS EXCEPTION HANDLING
            logger.exception(f"Error in _run_recording for {info.id}: {e}")
            info.state = RecordingState.FAILED
            info.error_message = str(e)
            raise
    
    def _watchdog_worker(self, info: RecordingInfo):
        """Watchdog to monitor recording health"""
        download_path = get_download_path()
        ts_path = os.path.join(download_path, f"{info.filename}.ts")
        
        last_size = 0
        last_change = time.time()
        stall_timeout = int(os.getenv('STALL_TIMEOUT_SECONDS', '480'))  # 8 minutes
        max_duration = int(os.getenv('MAX_RECORDING_DURATION', '28800'))  # 8 hours
        
        while not info.stop_event.is_set() and info.process and info.process.poll() is None:
            try:
                # Check for stop signal
                if info.stop_event.wait(15):  # Check every 15 seconds
                    break
                
                # Check max duration
                if info.started_at and (datetime.utcnow() - info.started_at).total_seconds() > max_duration:
                    logger.info(f"Recording {info.id} hit max duration, stopping")
                    info.stop_event.set()
                    break
                
                # Check file growth (stall detection)
                current_size = 0
                if os.path.exists(ts_path):
                    try:
                        current_size = os.path.getsize(ts_path)
                    except OSError:
                        pass
                
                if current_size > last_size:
                    last_size = current_size
                    last_change = time.time()
                elif time.time() - last_change > stall_timeout:
                    logger.info(f"Recording {info.id} stalled (no growth for {stall_timeout}s), stopping")
                    info.stop_event.set()
                    break
                
            except Exception as e:
                logger.exception(f"Watchdog error for recording {info.id}: {e}")
                break
    
    def _finalize_recording(self, info: RecordingInfo):
        """Finalize recording and update database"""
        try:
            with self.app.app_context():
                from app import Recording  # Import here to avoid circular imports
                
                recording = Recording.query.get(info.id)
                if recording:
                    # Determine final status
                    if info.state == RecordingState.COMPLETED:
                        recording.status = 'completed'
                        recording.status_detail = 'Completed successfully'
                    elif info.state == RecordingState.STOPPING:
                        recording.status = 'stopped'
                        recording.status_detail = 'Stopped by user'
                    else:
                        recording.status = 'failed'
                        recording.status_detail = info.error_message or 'Recording failed'
                    
                    recording.ended_at = datetime.utcnow()
                    
                    # Calculate duration
                    if recording.started_at:
                        duration = (recording.ended_at - recording.started_at).total_seconds()
                        recording.duration = int(duration)
                    
                    # Set file size
                    ts_path = os.path.join(get_download_path(), f"{recording.filename}.ts")
                    if os.path.exists(ts_path):
                        try:
                            recording.file_size = os.path.getsize(ts_path)
                        except OSError:
                            pass
                    
                    self.db.session.commit()
                    logger.info(f"Finalized recording {info.id} with status {recording.status}")
                
        except Exception as e:
            logger.exception(f"Error finalizing recording {info.id}: {e}")
        finally:
            # Always clean up tracking data
            with self._lock:
                self._cleanup_recording_unsafe(info.id)
    
    def _cleanup_recording_unsafe(self, recording_id: int):
        """Clean up recording data (must be called with lock held)"""
        info = self._recordings.pop(recording_id, None)
        if info:
            # Remove from streamer mapping
            if info.streamer_id in self._streamer_recordings:
                if self._streamer_recordings[info.streamer_id] == recording_id:
                    self._streamer_recordings.pop(info.streamer_id, None)
            
            logger.debug(f"Cleaned up recording {recording_id}")
    
    def _make_filename(self, streamer) -> str:
        """Generate filename for recording"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        return f"{streamer.twitch_name} - {timestamp}"
    
    
    def shutdown(self):
        """Shutdown the recording manager"""
        logger.info("Shutting down recording manager")
        self._running = False
        
        # Stop all active recordings
        with self._lock:
            active_recordings = list(self._recordings.keys())
        
        for recording_id in active_recordings:
            self.stop_recording(recording_id)
        
        # Wait for cleanup thread
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)


# Helper functions for app.py integration
def get_download_path():
    """Get the download path from environment or use default"""
    return os.getenv('DOWNLOAD_PATH', './download')


# Global recording manager instance (will be initialized in app.py)
recording_manager: Optional[RecordingManager] = None


def init_recording_manager(app, db):
    """Initialize the global recording manager"""
    global recording_manager
    recording_manager = RecordingManager(app, db)
    return recording_manager


def get_recording_manager() -> RecordingManager:
    """Get the global recording manager"""
    if recording_manager is None:
        raise RuntimeError("Recording manager not initialized")
    return recording_manager
