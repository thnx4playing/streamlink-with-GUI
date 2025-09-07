#!/usr/bin/env python3
"""
Fixed Recording Manager with proper OAuth implementation
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
    
    # Store database data to avoid queries in thread
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
        logger.info(f"🎬 START_RECORDING called for streamer {streamer_id}")
        
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
                    logger.info(f"Cleaning up stale recording {existing_id} for streamer {streamer_id}")
                    self._cleanup_recording_unsafe(existing_id)
            
            # Create new recording in database
            with self.app.app_context():
                try:
                    from app import Streamer, Recording, TwitchAuth  # Import here to avoid circular imports
                    
                    # GET ALL DATABASE DATA FIRST (before starting thread)
                    streamer = Streamer.query.get(streamer_id)
                    if not streamer:
                        logger.error(f"❌ Streamer {streamer_id} not found")
                        return None
                    
                    logger.info(f"✅ Found streamer: {streamer.username} (Twitch: {streamer.twitch_name})")
                    
                    # Get auth data before starting thread
                    auth = TwitchAuth.query.first()
                    logger.info(f"🔑 Auth data found: {bool(auth)}")
                    if auth:
                        logger.info(f"🔑 Has OAuth token: {bool(auth.oauth_token)}")
                        logger.info(f"🔑 Has Client ID: {bool(auth.client_id)}")
                    
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
                    logger.info(f"📝 Created recording record with ID: {recording_id}")
                    
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
                    info.streamer_quality = streamer.quality or 'best'
                    info.auth_data = {
                        'oauth_token': auth.oauth_token if auth else None,
                        'client_id': auth.client_id if auth else None,
                        'client_secret': auth.client_secret if auth else None,
                        'extra_flags': auth.extra_flags if auth else None,
                        'enable_hls_live_restart': getattr(auth, 'enable_hls_live_restart', False) if auth else False
                    }
                    
                    logger.info(f"🎯 Recording info prepared - Twitch name: {info.streamer_twitch_name}, Quality: {info.streamer_quality}")
                    
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
                    logger.info(f"🚀 Started recording thread for recording {recording_id}")
                    
                    logger.info(f"✅ Successfully started recording {recording_id} for streamer {streamer_id}")
                    return recording_id
                    
                except Exception as e:
                    logger.exception(f"❌ Error starting recording for streamer {streamer_id}: {e}")
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
    
    def is_recording_active(self, recording_id: int) -> bool:
        """Check if a specific recording is active"""
        with self._lock:
            info = self._recordings.get(recording_id)
            return info and info.state in (RecordingState.STARTING, RecordingState.RECORDING)
    
    def _recording_worker(self, info: RecordingInfo):
        """Main recording worker thread"""
        logger.info(f"🔧 WORKER: Starting recording worker for recording {info.id}")
        try:
            with self.app.app_context():
                logger.info(f"🔧 WORKER: Entering app context for recording {info.id}")
                self._run_recording(info)
            logger.info(f"🔧 WORKER: Recording completed for recording {info.id}")
        except Exception as e:
            logger.exception(f"❌ WORKER: Recording worker error for {info.id}: {e}")
            info.state = RecordingState.FAILED
            info.error_message = str(e)
        finally:
            logger.info(f"🔧 WORKER: Finalizing recording {info.id}")
            self._finalize_recording(info)
    
    def _run_recording(self, info: RecordingInfo):
        """Run the actual recording process using existing StreamlinkManager"""
        logger.info(f"🎥 RECORDING: Starting recording process for {info.id}")
        
        from app import get_download_path
        from streamlink_manager import StreamlinkManager  # Import your working class
        
        try:
            # Use the data that was passed from start_recording
            streamer_twitch_name = info.streamer_twitch_name
            streamer_quality = info.streamer_quality
            auth_data = info.auth_data
            
            logger.info(f"🎯 Recording {info.id} - Streamer: {streamer_twitch_name}, Quality: {streamer_quality}")
            
            # Build output path
            download_path = get_download_path()
            os.makedirs(download_path, exist_ok=True)
            
            base_path = os.path.join(download_path, info.filename)
            # Note: StreamlinkManager will add the .ts extension
            
            logger.info(f"📁 Output path: {base_path}")
            
            # Create a config object like your existing AppConfig
            class RecordingConfig:
                def __init__(self, auth_data):
                    self.oauth_token = auth_data.get('oauth_token') if auth_data else None
                    self.quality = streamer_quality
            
            config = RecordingConfig(auth_data)
            
            # Create StreamlinkManager instance (your working code!)
            streamlink_manager = StreamlinkManager(config)
            
            logger.info(f"🔑 Using OAuth token: {bool(config.oauth_token)}")
            if config.oauth_token:
                logger.info(f"🔑 OAuth token length: {len(config.oauth_token)}")
            
            # Update state
            info.state = RecordingState.RECORDING
            
            logger.info(f"🚀 RECORDING: Starting streamlink for {streamer_twitch_name}")
            
            # Use your existing working run_streamlink method!
            result = streamlink_manager.run_streamlink(
                user=streamer_twitch_name,
                recorded_filename=base_path,  # StreamlinkManager will add .ts
                stop_event=info.stop_event,
                logger=logger
            )
            
            logger.info(f"⏹️ RECORDING: Streamlink finished for {info.id}")
            logger.info(f"📊 Result: {result}")
            
            # Check the result
            if result.get("stopped_by_user"):
                info.state = RecordingState.STOPPING
            else:
                # Check if final file exists
                final_file = result.get("final")
                if final_file and os.path.exists(final_file) and os.path.getsize(final_file) > 0:
                    info.state = RecordingState.COMPLETED
                else:
                    info.state = RecordingState.FAILED
                    info.error_message = "No output file or empty file"
                    
        except Exception as e:
            logger.exception(f"❌ RECORDING: Error in _run_recording for {info.id}: {e}")
            info.state = RecordingState.FAILED
            info.error_message = str(e)
            raise
    
    
    def _finalize_recording(self, info: RecordingInfo):
        """Finalize recording and update database"""
        logger.info(f"🏁 FINALIZE: Starting finalization for recording {info.id}")
        
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
                    logger.info(f"🏁 FINALIZE: Recording {info.id} finalized with status {recording.status}")
                else:
                    logger.error(f"🏁 FINALIZE: Recording {info.id} not found in database")
                
        except Exception as e:
            logger.exception(f"🏁 FINALIZE: Error finalizing recording {info.id}: {e}")
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
            
            logger.debug(f"🧹 Cleaned up recording {recording_id}")
    
    def _make_filename(self, streamer) -> str:
        """Generate filename for recording"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        return f"{streamer.twitch_name} - {timestamp}"
    
    def shutdown(self):
        """Shutdown the recording manager"""
        logger.info("🛑 Shutting down recording manager")
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


# Use Flask's application context instead of global variables
from flask import current_app


def get_recording_manager():
    """Get the recording manager, creating it if needed"""
    if not hasattr(current_app, 'recording_manager'):
        # Initialize if not exists
        current_app.recording_manager = RecordingManager(current_app._get_current_object(), current_app.extensions['sqlalchemy'].db)
    return current_app.recording_manager


def init_recording_manager(app, db):
    """Initialize the recording manager and store it on the app"""
    if not hasattr(app, 'recording_manager'):
        app.recording_manager = RecordingManager(app, db)
    return app.recording_manager