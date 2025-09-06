#!/usr/bin/env python3
"""
Streamlink Web GUI - A web interface for managing Twitch stream recordings
"""
import os, sys, time, json, threading, subprocess, signal, uuid, logging, shutil, socket, getpass, atexit
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from sqlalchemy.orm import scoped_session, sessionmaker

# Import existing modules
from twitch_manager import TwitchManager, StreamStatus
from streamlink_manager import StreamlinkManager
from notification_manager import NotificationManager
from recording_manager import init_recording_manager, get_recording_manager, RecordingState
from stream_monitor import init_stream_monitor, get_stream_monitor

# === CONTINUOUS MODE (behave like your stable Streamlink container) ===
STRICT_CONTINUOUS_MODE = os.getenv("STRICT_CONTINUOUS_MODE", "1") not in ("0", "false", "False")
OFFLINE_FINALIZE_SECONDS = int(os.getenv("OFFLINE_FINALIZE_SECONDS", "900"))  # 15m sustained offline before finalize (only if proc already died)

# Conservative thresholds (watchdog will not kill on these when STRICT_CONTINUOUS_MODE=True)
STALL_TIMEOUT_SECONDS = int(os.getenv("STALL_TIMEOUT_SECONDS", "480"))   # 8m no growth = stall signal (log-only in strict mode)
OFFLINE_CHECK_PERIOD  = int(os.getenv("OFFLINE_CHECK_PERIOD", "15"))     # seconds

# Legacy config (kept for compatibility)
RESUME_WITHIN_MINUTES = int(os.getenv("RESUME_WITHIN_MINUTES", "5"))     # allow resume within 3–5m
OFFLINE_GRACE_SECONDS = int(os.getenv("OFFLINE_GRACE_SECONDS", "300"))   # require ~5m of offline before finalizing

# Optional: enable streamlink debug logs (much chattier)
STREAMLINK_DEBUG = os.getenv("STREAMLINK_DEBUG", "0") in ("1","true","True")

# Watchdog helper functions for recording management
def _safe_terminate(proc: subprocess.Popen, timeout=15):
    """Safely terminate a process with timeout fallback to kill"""
    if proc and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except Exception:
                pass

def _file_size(path):
    """Get file size safely, returns -1 on error"""
    try:
        return os.path.getsize(path)
    except Exception:
        return -1

# === PATCH 2/5: Helpers ===
_stop_reason = {}  # recording_id -> reason string
_stop_reason_lock = threading.Lock()

def _set_stop_reason(rec_id: int, reason: str):
    with _stop_reason_lock:
        _stop_reason[rec_id] = reason

def _pop_stop_reason(rec_id: int) -> str | None:
    with _stop_reason_lock:
        return _stop_reason.pop(rec_id, None)

def _is_twitch_live_simple(twitch_user: str) -> bool:
    """Lightweight 'is live' check; tolerate API errors."""
    try:
        user = twitch_manager.get_from_twitch('get_users', logins=twitch_user)
        if not user:
            return False
        stream = twitch_manager.get_from_twitch('get_streams', user_id=user.id)
        return bool(stream and getattr(stream, "type", "") == "live")
    except Exception as e:
        logger.warning("is_live check failed for %s: %s", twitch_user, e)
        return False

# _concat_parts_to_final function removed - now handled by RecordingManager

def make_safe_filename(title: str) -> str:
    """Create a safe filename from a title"""
    return "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()

def build_recording_filename(twitch_user: str, safe_title: str, when: datetime) -> str:
    """Build recording filename with timestamp"""
    timestamp = when.strftime('%Y-%m-%d %H-%M-%S')
    return f"{twitch_user} - {timestamp} - {safe_title}"

# start_recording_watchdog function removed - now handled by RecordingManager

def _is_twitch_live(username: str) -> bool:
    """Check if a Twitch streamer is currently live"""
    try:
        # Get the first streamer to use their config (assuming they all use same Twitch API credentials)
        streamer = Streamer.query.first()
        if not streamer:
            return True  # fail-open if no streamers configured
        
        config = AppConfig(streamer)
        twitch_manager = TwitchManager(config)
        status, _ = twitch_manager.check_user(username)
        return status == StreamStatus.ONLINE
    except Exception:
        return True  # fail-open to avoid flapping on API hiccups

# _watchdog_stop_when_idle_or_offline function removed - now handled by RecordingManager

# _salvage_recording_files function removed - now handled by RecordingManager

# _bulletproof_finalize_recording function removed - now handled by RecordingManager

# Load environment variables
load_dotenv()

# Configure logging to file and stdout
log_dir = '/app/logs'
os.makedirs(log_dir, exist_ok=True)
logger = logging.getLogger("streamlink-webgui")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s")
    fh = RotatingFileHandler(f"{log_dir}/app.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(_fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(_fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)

# Configure dedicated conversion logger
conversion_logger = logging.getLogger("conversion")
conversion_logger.setLevel(logging.INFO)
if not conversion_logger.handlers:
    conv_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(threadName)s CONVERSION: %(message)s")
    conv_fh = RotatingFileHandler(f"{log_dir}/conversion.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    conv_fh.setFormatter(conv_fmt)
    conv_sh = logging.StreamHandler()
    conv_sh.setFormatter(conv_fmt)
    conversion_logger.addHandler(conv_fh)
    conversion_logger.addHandler(conv_sh)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-change-this')

# Always use a fixed internal path for the database
# Docker volumes will handle the mapping to host paths
data_dir = '/app/data'

# Debug: Check current user and permissions
import pwd
try:
    current_user = pwd.getpwuid(os.getuid()).pw_name
    logger.info(f"Running as user: {current_user} (UID: {os.getuid()})")
except:
    logger.info(f"Running as UID: {os.getuid()}")

# Check if directory exists and is writable
logger.info(f"Data directory: {data_dir}")
logger.info(f"Directory exists: {os.path.exists(data_dir)}")
logger.info(f"Directory writable: {os.access(data_dir, os.W_OK) if os.path.exists(data_dir) else 'N/A'}")

# Try to create directory
try:
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Successfully created/accessed data directory: {data_dir}")
except Exception as e:
    logger.error(f"Failed to create data directory: {e}")

# Use data directory for database (persistent volume)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{data_dir}/streamlink.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# SQLite engine options for thread safety and busy timeout
app.config.setdefault("SQLALCHEMY_ENGINE_OPTIONS", {
    "pool_pre_ping": True,
    "pool_size": 20,                  # Increased from 5 to handle more concurrent requests
    "max_overflow": 30,               # Increased from 5 to allow more overflow connections
    "pool_timeout": 60,               # Increased timeout for getting connections from pool
    "pool_recycle": 3600,             # Recycle connections after 1 hour
    "connect_args": {
        "check_same_thread": False,   # required since we use threads
        "timeout": 60,                # increased sqlite busy timeout in seconds
    }
})

# Initialize extensions
db = SQLAlchemy(app)
CORS(app)

def cleanup_session(f):
    """Decorator to ensure proper session cleanup after API calls"""
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            # Log the error but don't re-raise to avoid double logging
            app.logger.error(f"Error in {f.__name__}: {e}")
            raise
        finally:
            # Always cleanup the session
            try:
                db.session.remove()
            except Exception as cleanup_error:
                app.logger.warning(f"Session cleanup error in {f.__name__}: {cleanup_error}")
    wrapper.__name__ = f.__name__
    return wrapper

def log_connection_pool_status():
    """Log current connection pool status for debugging"""
    try:
        pool = db.engine.pool
        app.logger.info(f"Connection pool status: size={pool.size()}, checked_in={pool.checkedin()}, checked_out={pool.checkedout()}, overflow={pool.overflow()}")
    except Exception as e:
        app.logger.warning(f"Could not log connection pool status: {e}")

# Create scoped session for worker threads (will be initialized later)
WorkerSession = None

def initialize_worker_session():
    """Initialize the worker session within app context"""
    global WorkerSession
    WorkerSession = scoped_session(sessionmaker(bind=db.engine))

# Configure SQLite for better concurrency (WAL mode and busy timeout)
from sqlalchemy import text, event

def _is_sqlite_uri(uri: str) -> bool:
    return uri.startswith("sqlite:")

def _set_sqlite_pragma(dbapi_connection, connection_record):
    uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
    if not _is_sqlite_uri(uri):
        return
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA busy_timeout=60000;")
    cursor.close()

def setup_sqlite_event_listeners():
    """Setup SQLite event listeners within app context"""
    event.listens_for(db.engine, "connect")(_set_sqlite_pragma)

# Also set initial PRAGMAs for existing connections and initialize worker session
with app.app_context():
    try:
        # Setup SQLite event listeners
        setup_sqlite_event_listeners()
        
        db.session.execute(text("PRAGMA journal_mode=WAL;"))
        db.session.execute(text("PRAGMA synchronous=NORMAL;"))
        db.session.execute(text("PRAGMA busy_timeout=60000;"))
        db.session.commit()
        
        # Initialize scoped session for worker threads
        initialize_worker_session()
        
    except Exception as e:
        app.logger.warning(f"SQLite PRAGMA init failed: {e}")

# --- FFmpeg Presets used by the conversion worker ---
FFMPEG_PRESETS = {
    "default_h264_aac": {
        "label": "Default – H.264 CRF 23 / AAC 128k (preset=medium)",
        "args": ["-c:v","libx264","-preset","medium","-crf","23","-c:a","aac","-b:a","128k","-movflags","+faststart"],
        "desc": "Good quality, much smaller than source. Recommended default."
    },
    "smaller_h264_aac": {
        "label": "Smaller – H.264 CRF 26 / AAC 96k (preset=slow)",
        "args": ["-c:v","libx264","-preset","slow","-crf","26","-c:a","aac","-b:a","96k","-movflags","+faststart"],
        "desc": "Prioritizes smaller files with a modest quality hit. Slower encode."
    },
    "higher_h264_aac": {
        "label": "Higher Quality – H.264 CRF 20 / AAC 160k (preset=slow)",
        "args": ["-c:v","libx264","-preset","slow","-crf","20","-c:a","aac","-b:a","160k","-movflags","+faststart"],
        "desc": "Better quality, larger files. Slower encode."
    },
    "cap720_h264_aac": {
        "label": "720p Cap – H.264 CRF 23 / AAC 128k + downscale to 720p",
        "args": ["-vf","scale=-2:720","-c:v","libx264","-preset","medium","-crf","23","-c:a","aac","-b:a","128k","-movflags","+faststart"],
        "desc": "Downscales tall videos to max 720p height to save space."
    },
    "remux_copy": {
        "label": "Remux Only – Copy streams (no re-encode)",
        "args": ["-c:v","copy","-c:a","copy","-movflags","+faststart"],
        "desc": "Fastest. No size/quality change. Use when source is already H.264/AAC."
    },
}
DEFAULT_FFMPEG_PRESET_KEY = "default_h264_aac"

def _ensure_conversion_settings_columns():
    """Add ffmpeg_preset column if not present (SQLite compatible)"""
    try:
        cols = [r[1] for r in db.session.execute(text("PRAGMA table_info(conversion_settings)")).fetchall()]
        if "ffmpeg_preset" not in cols:
            # Use direct string value instead of parameter for SQLite compatibility
            db.session.execute(text(f"ALTER TABLE conversion_settings ADD COLUMN ffmpeg_preset VARCHAR(64) NOT NULL DEFAULT '{DEFAULT_FFMPEG_PRESET_KEY}'"))
            db.session.commit()
            logger.info("Added ffmpeg_preset column to conversion_settings table")
    except Exception as e:
        logger.warning(f"Schema migration failed: {e}")

# Run schema migration within app context
with app.app_context():
    _ensure_conversion_settings_columns()

# Database Models
class Streamer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    twitch_name = db.Column(db.String(80), nullable=False)
    quality = db.Column(db.String(20), default='best')
    timer = db.Column(db.Integer, default=360)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    # Add these for monitoring
    title = db.Column(db.String(255))  # Current stream title
    game = db.Column(db.String(255))   # Current game being played

class Recording(db.Model):
    __table_args__ = (
        db.Index('ix_rec_status_started', 'status', 'started_at'),
    )
    id = db.Column(db.Integer, primary_key=True)
    streamer_id = db.Column(db.Integer, db.ForeignKey('streamer.id'), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    title = db.Column(db.String(255))
    status = db.Column(db.String(20), default='recording')  # recording, completed, stopped, failed, deleted
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    ended_at = db.Column(db.DateTime)
    file_size = db.Column(db.Integer)  # in bytes
    duration = db.Column(db.Integer)  # in seconds
    game = db.Column(db.String(255))  # Game/category being played
    pid = db.Column(db.Integer, nullable=True)
    session_guid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()))
    status_detail = db.Column(db.String(255))  # Details about why recording stopped (e.g., "Stopped by user", "Stream offline", "No data")

class ConversionSettings(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    volume_path = db.Column(db.String(500))
    output_volume_path = db.Column(db.String(500))  # New: separate output directory
    naming_scheme = db.Column(db.String(50), default='streamer_date_title')
    custom_filename_template = db.Column(db.String(500))  # New: custom naming template
    delete_original_after_conversion = db.Column(db.Boolean, default=False)  # New: delete original option
    ffmpeg_preset = db.Column(db.String(64), nullable=False, default=DEFAULT_FFMPEG_PRESET_KEY)  # New: ffmpeg preset key
    # Watchdog settings for recording management
    watchdog_offline_grace_s = db.Column(db.Integer, default=90)  # Grace period for offline detection (seconds)
    watchdog_stall_timeout_s = db.Column(db.Integer, default=180)  # File stall timeout (seconds)
    watchdog_max_duration_s = db.Column(db.Integer, default=8*3600)  # Max recording duration (seconds)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ConversionJob(db.Model):
    __table_args__ = (
        db.Index('ix_cjob_status_sched', 'status', 'scheduled_at'),
        db.Index('ix_cjob_rec_status', 'recording_id', 'status'),
    )
    id = db.Column(db.Integer, primary_key=True)
    recording_id = db.Column(db.Integer, db.ForeignKey('recording.id'), nullable=True)  # Allow NULL for template jobs
    status = db.Column(db.String(20), default='pending')  # pending, scheduled, converting, completed, failed
    progress = db.Column(db.String(255))
    output_filename = db.Column(db.String(500))
    scheduled_at = db.Column(db.DateTime)  # New: scheduled time
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime)
    schedule_type = db.Column(db.String(20))  # New: immediate, daily, weekly, custom
    custom_filename = db.Column(db.String(500))  # New: custom filename for this job
    delete_original = db.Column(db.Boolean, default=False)  # New: delete original for this job

class TwitchAuth(db.Model):
    __tablename__ = 'twitch_auth'
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.String(128), nullable=True)
    client_secret = db.Column(db.String(256), nullable=True)
    oauth_token = db.Column(db.String(512), nullable=True)
    extra_flags = db.Column(db.String(512), nullable=True)  # e.g. "--twitch-low-latency"
    enable_hls_live_restart = db.Column(db.Boolean, default=False)  # Enable --hls-live-restart flag
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AppConfig:
    def __init__(self, streamer):
        self.timer = streamer.timer
        self.user = streamer.twitch_name
        self.quality = streamer.quality
        self.client_id = os.getenv('TWITCH_CLIENT_ID')
        self.client_secret = os.getenv('TWITCH_CLIENT_SECRET')
        self.oauth_token = os.getenv('TWITCH_OAUTH_TOKEN')
        self.slack_id = os.getenv('SLACK_ID')
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.game_list = os.getenv('GAME_LIST', '')

# Global variables for managing conversion processes
recording_processes = {}
conversion_worker_thread = None
conversion_wakeup = threading.Event()

# Track conversion processes for hard-stop fallback
conversion_processes = {}  # {job_id: {"proc": Popen, "pid": int}}

def ensure_database_migrated():
    """Ensure database schema is up to date with all required columns"""
    try:
        with db.engine.connect() as conn:
            # Check if status_detail column exists in recording table
            result = conn.execute(db.text("PRAGMA table_info(recording)"))
            columns = [row[1] for row in result.fetchall()]
            
            # Add missing columns to recording table
            missing_columns = []
            if 'status_detail' not in columns:
                missing_columns.append('status_detail')
            
            for col in missing_columns:
                if col == 'status_detail':
                    conn.execute(db.text("ALTER TABLE recording ADD COLUMN status_detail VARCHAR(255)"))
                    logger.info("Added status_detail column to recording table")
            
            # Check twitch_auth table
            result = conn.execute(db.text("PRAGMA table_info(twitch_auth)"))
            auth_columns = [row[1] for row in result.fetchall()]
            
            if 'enable_hls_live_restart' not in auth_columns:
                conn.execute(db.text("ALTER TABLE twitch_auth ADD COLUMN enable_hls_live_restart BOOLEAN DEFAULT 0"))
                logger.info("Added enable_hls_live_restart column to twitch_auth table")
            
            # Check conversion_settings table
            result = conn.execute(db.text("PRAGMA table_info(conversion_settings)"))
            settings_columns = [row[1] for row in result.fetchall()]
            
            watchdog_columns = ['watchdog_offline_grace_s', 'watchdog_stall_timeout_s', 'watchdog_max_duration_s']
            for col in watchdog_columns:
                if col not in settings_columns:
                    if col == 'watchdog_offline_grace_s':
                        conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN watchdog_offline_grace_s INTEGER DEFAULT 90"))
                    elif col == 'watchdog_stall_timeout_s':
                        conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN watchdog_stall_timeout_s INTEGER DEFAULT 180"))
                    elif col == 'watchdog_max_duration_s':
                        conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN watchdog_max_duration_s INTEGER DEFAULT 28800"))
                    logger.info(f"Added {col} column to conversion_settings table")
            
            # Check streamer table for new monitoring fields
            result = conn.execute(db.text("PRAGMA table_info(streamer)"))
            streamer_columns = [row[1] for row in result.fetchall()]
            
            streamer_missing_columns = []
            if 'title' not in streamer_columns:
                streamer_missing_columns.append('title')
            if 'game' not in streamer_columns:
                streamer_missing_columns.append('game')
            
            for col in streamer_missing_columns:
                if col == 'title':
                    conn.execute(db.text("ALTER TABLE streamer ADD COLUMN title VARCHAR(255)"))
                    logger.info("Added title column to streamer table")
                elif col == 'game':
                    conn.execute(db.text("ALTER TABLE streamer ADD COLUMN game VARCHAR(255)"))
                    logger.info("Added game column to streamer table")
            
            conn.commit()
            logger.info("Database migration check completed")
            
    except Exception as e:
        logger.error(f"Database migration check failed: {e}")
        # Don't raise the exception, just log it

def get_download_path():
    """Get the download path from environment or use default"""
    return os.getenv('DOWNLOAD_PATH', './download')

# === PATCH: structured logging & constants ===
HOSTNAME = socket.gethostname()

def jlog(logger, event: str, **kv):
    """Emit a structured JSON log line (single line, easy to grep/parse)."""
    payload = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "host": HOSTNAME,
        "event": event,
        **kv
    }
    try:
        logger.info("JLOG %s", json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        # fall back
        logger.info("JLOG %s %s", event, kv)

def human_bytes(n: int) -> str:
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024: return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"

def log_startup_diagnostics():
    """Call once at app boot to capture environment & toolchain."""
    try:
        import subprocess
        sl_ver = subprocess.check_output(["streamlink","--version"], text=True).strip() if shutil.which("streamlink") else "missing"
    except Exception as e:
        sl_ver = f"error:{e}"
    ffmpeg = shutil.which("ffmpeg") or "missing"
    uid_gid = f"{os.getuid()}:{os.getgid()}" if hasattr(os, "getuid") else getpass.getuser()
    jlog(logger, "startup",
        streamlink_version=sl_ver,
        ffmpeg=ffmpeg,
        python=os.sys.version.split()[0],
        uid_gid=uid_gid,
        download_path=get_download_path(),
        converted_path=get_converted_path(),
        strict_continuous_mode=STRICT_CONTINUOUS_MODE,
        stall_timeout_s=STALL_TIMEOUT_SECONDS,
        offline_check_period_s=OFFLINE_CHECK_PERIOD,
        offline_finalize_s=OFFLINE_FINALIZE_SECONDS,
        streamlink_debug=STREAMLINK_DEBUG
    )

def _validate_container_paths():
    """Validate that container paths are correctly configured to avoid mount issues"""
    download_path = os.getenv('DOWNLOAD_PATH', './download')
    if not download_path.startswith('/app/'):
        logger.warning(f"DOWNLOAD_PATH={download_path} is not inside the container volume (/app/). This can cause missing files if the path is not properly mounted.")
        logger.warning("Recommended: Set DOWNLOAD_PATH=/app/download in your .env file")
    
    # Also validate converted path consistency
    converted_path = get_converted_path()
    if not converted_path.startswith('/app/'):
        logger.warning(f"CONVERTED_PATH={converted_path} is not inside the container volume (/app/). This can cause conversion issues.")
        
    logger.info(f"Container paths validated - DOWNLOAD_PATH={download_path}, CONVERTED_PATH={converted_path}")

def get_converted_path():
    """Get the converted files path from environment or use default"""
    # Always use the internal container path for converted files
    return '/app/converted'

def get_download_volume_path():
    """Get the real host download path from environment or use default"""
    return os.getenv('DOWNLOAD_VOLUME_PATH', get_download_path())

def get_converted_volume_path():
    """Get the real host converted path from environment or use default"""
    return os.getenv('CONVERTED_VOLUME_PATH', get_converted_path())

def _utcnow():
    """Get current UTC time (naive)"""
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _build_ffmpeg_cmd(input_path, output_path, settings: ConversionSettings):
    """Build FFmpeg command using the selected preset"""
    key = getattr(settings, "ffmpeg_preset", DEFAULT_FFMPEG_PRESET_KEY)
    preset = FFMPEG_PRESETS.get(key, FFMPEG_PRESETS[DEFAULT_FFMPEG_PRESET_KEY])

    # Log which preset is being used
    conversion_logger.info(f"Building FFmpeg command with preset: {key} -> {preset['label']}")

    # Always include -y and -threads 0; then append preset args
    cmd = ["ffmpeg", "-hide_banner", "-y", "-i", input_path, "-threads", "0"]
    cmd += preset["args"]
    cmd += [output_path]
    
    # Log the full command for debugging
    conversion_logger.info(f"FFmpeg command: {' '.join(cmd)}")
    
    return cmd

def _retry_locked(fn, *args, **kwargs):
    """Retry function with backoff for transient 'database is locked' errors"""
    from sqlalchemy.exc import OperationalError
    for i in range(5):
        try:
            return fn(*args, **kwargs)
        except OperationalError as e:
            if "database is locked" in str(e).lower():
                time.sleep(0.2 * (i + 1))
                continue
            raise

def _eligible_conversion_jobs(session, batch_size=None):
    if batch_size is None:
        # Get concurrency from environment variable, default to 1
        batch_size = int(os.getenv('CONVERSION_CONCURRENCY', '1'))
    """Return next chunk of conversion jobs that should run now."""
    now = _utcnow()
    
    # Count active converting jobs to respect concurrency limit
    active_count = (
        session.query(ConversionJob)
        .filter(ConversionJob.status == 'converting')
        .count()
    )
    
    # If we're at capacity, don't pick up new jobs
    if active_count >= batch_size:
        return []
    
    # Pick the oldest eligible pending job
    job = (
        session.query(ConversionJob)
        .filter(ConversionJob.status == 'pending')
        .filter(
            db.or_(
                # Immediate jobs: run regardless of scheduled_at
                ConversionJob.schedule_type == 'immediate',
                # Scheduled jobs: run when due
                db.and_(
                    ConversionJob.schedule_type == 'scheduled',
                    ConversionJob.scheduled_at.isnot(None),
                    ConversionJob.scheduled_at <= now,
                ),
            )
        )
        .order_by(ConversionJob.id.asc())
        .first()
    )
    return [job] if job else []

def _update_job_progress(job, text, session, *, force=False):
    """
    Throttled to ~1/sec unless force=True. Keeps short 'progress' text in DB.
    """
    text = (text or "")[:255]
    now = _utcnow()
    if not force:
        # optional: store a per-job "last_progress_at" in memory or on the job
        last = getattr(job, "_last_progress_at", None)
        if last and (now - last).total_seconds() < 1:
            return
        job._last_progress_at = now

    # Re-attach to ensure we work with an attached instance
    job = session.get(ConversionJob, job.id)
    job.progress = text
    session.add(job)
    try:
        session.commit()
    except Exception:
        session.rollback()

def _start_job(job: "ConversionJob", session):
    """Mark job as started and commit quickly"""
    # Re-attach to ensure we work with an attached instance
    job = session.get(ConversionJob, job.id)
    job.status = 'converting'
    job.started_at = _utcnow()
    _update_job_progress(job, "Starting conversion…", session, force=True)

def _finalize_job(job: "ConversionJob", session, *, ok: bool, done_text: str = None):
    """Mark job as completed/failed and commit quickly"""
    # Re-attach to ensure we work with an attached instance
    job = session.get(ConversionJob, job.id)
    job.completed_at = _utcnow()
    job.status = 'completed' if ok else 'failed'
    if done_text:
        _update_job_progress(job, done_text, session, force=True)
    session.add(job)
    session.commit()

def _run_ffmpeg_conversion(job: "ConversionJob", session=None):
    """Run the actual FFmpeg conversion (long-running operation)"""
    if session is None:
        session = db.session
    # Resolve paths & validations (kept defensive)
    rec = Recording.query.get(job.recording_id) if job.recording_id else None
    if not rec:
        # Re-attach to ensure we work with an attached instance
        job = session.get(ConversionJob, job.id)
        job.status = 'failed'
        _update_job_progress(job, 'Recording not found', session)
        return
    # Only finalized .ts (no .part)
    ts_path = os.path.join(get_download_path(), f"{rec.filename}.ts")
    part_path = os.path.join(get_download_path(), f"{rec.filename}.ts.part")
    if (not os.path.exists(ts_path)) or os.path.exists(part_path) or rec.status in ('deleted','failed','recording'):
        # Re-attach to ensure we work with an attached instance
        job = session.get(ConversionJob, job.id)
        job.status = 'failed'
        _update_job_progress(job, 'Source not eligible', session)
        return
    out_dir = get_converted_path()
    
    # Get conversion settings for naming template
    settings = session.query(ConversionSettings).first()
    if not settings:
        settings = ConversionSettings()
        session.add(settings)
        session.commit()
    
    # Build filename using conversion settings template or fallback to job custom filename
    if settings.custom_filename_template and settings.custom_filename_template.strip():
        # Use the global conversion settings template
        base_name = build_custom_filename(settings.custom_filename_template, rec)
        conversion_logger.info(f"Using conversion settings template: {settings.custom_filename_template}")
    elif job.custom_filename and job.custom_filename.strip():
        # Use job-specific custom filename
        base_name = job.custom_filename.strip()
        conversion_logger.info(f"Using job-specific custom filename: {base_name}")
    else:
        # Fallback to original filename
        base_name = rec.filename
        conversion_logger.info(f"Using original filename: {base_name}")
    
    # Ensure it ends with .mp4
    if not base_name.endswith('.mp4'):
        base_name += '.mp4'
    
    mp4_path = os.path.join(out_dir, base_name)
    
    # Get conversion settings for preset (reuse settings from above)
    
    # Log the settings being used
    conversion_logger.info(f"Using conversion settings - ffmpeg_preset: {getattr(settings, 'ffmpeg_preset', 'NOT_SET')}")
    
    # Build ffmpeg cmd using preset
    cmd = _build_ffmpeg_cmd(ts_path, mp4_path, settings)
    _update_job_progress(job, "Starting ffmpeg…", session)
    
    # Enhanced logging for troubleshooting
    conversion_logger.info(f"=== CONVERSION JOB {job.id} START ===")
    conversion_logger.info(f"Input file: {ts_path}")
    conversion_logger.info(f"Output file: {mp4_path}")
    conversion_logger.info(f"Input exists: {os.path.exists(ts_path)}")
    conversion_logger.info(f"Input size: {os.path.getsize(ts_path) if os.path.exists(ts_path) else 'N/A'} bytes")
    conversion_logger.info(f"Output dir exists: {os.path.exists(out_dir)}")
    conversion_logger.info(f"FFmpeg command: {' '.join(cmd)}")
    conversion_logger.info(f"Delete original after conversion: {getattr(job, 'delete_original', False)}")
    
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        conversion_logger.info(f"FFmpeg process started with PID: {proc.pid}")
        
        # Store process handle for potential hard-stop
        conversion_processes[job.id] = {"proc": proc, "pid": proc.pid}
        
        # Throttle progress updates to reduce database writes
        last_commit = 0
        last_progress = ""
        
        def _maybe_update_progress(text):
            nonlocal last_commit, last_progress
            now = time.time()
            # Update if text changed or at most once per second
            if now - last_commit >= 1 or text != last_progress:
                _update_job_progress(job, text, session)
                last_commit = now
                last_progress = text
        
        # read a few lines to keep UI alive
        last_line = ''
        while True:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break;
            if line:
                last_line = line.strip()
                # Log all FFmpeg output to conversion.log
                conversion_logger.info(f"FFmpeg output: {last_line}")
                if 'time=' in last_line or 'frame=' in last_line:
                    _maybe_update_progress(last_line[-120:])
        rc = proc.wait()
        conversion_logger.info(f"FFmpeg process completed with return code: {rc}")
        
        # Enhanced completion logging
        output_exists = os.path.exists(mp4_path)
        output_size = os.path.getsize(mp4_path) if output_exists else 0
        input_size_after = os.path.getsize(ts_path) if os.path.exists(ts_path) else 0
        
        conversion_logger.info(f"=== CONVERSION JOB {job.id} RESULT ===")
        conversion_logger.info(f"FFmpeg return code: {rc}")
        conversion_logger.info(f"Output file exists: {output_exists}")
        conversion_logger.info(f"Output file size: {output_size} bytes")
        conversion_logger.info(f"Input file still exists: {os.path.exists(ts_path)}")
        conversion_logger.info(f"Input file size after: {input_size_after} bytes")
        
        if rc == 0 and output_exists:
            conversion_logger.info(f"✅ Conversion SUCCESSFUL: {os.path.basename(ts_path)} -> {os.path.basename(mp4_path)} ({output_size} bytes)")
            
            # Set the output filename for the job
            job.output_filename = os.path.basename(mp4_path)
            
            # Log file preservation/deletion clearly
            if job.delete_original and os.path.exists(ts_path):
                try: 
                    conversion_logger.info(f"🗑️ DELETE_ORIGINAL=True: Removing source file {ts_path}")
                    os.remove(ts_path)
                    conversion_logger.info(f"✅ Successfully deleted original file: {os.path.basename(ts_path)}")
                except Exception as e:
                    conversion_logger.warning(f"❌ Failed to delete original file {os.path.basename(ts_path)}: {e}")
            else:
                conversion_logger.info(f"💾 DELETE_ORIGINAL=False: Preserving source file {ts_path}")
                
            # Let the worker loop handle finalization
            return
        else:
            conversion_logger.error(f"❌ Conversion FAILED: FFmpeg return code {rc}, output exists: {output_exists}")
            raise Exception(f"ffmpeg failed (rc={rc})")
    except Exception as e:
        conversion_logger.error(f"Conversion exception: {e}")
        raise
    finally:
        # Clean up process tracking
        conversion_processes.pop(job.id, None)

def template_scheduler_loop():
    """Expand due schedule templates (recording_id is NULL) into real conversion jobs."""
    with app.app_context():
        logger.info("Template scheduler: started")
        while True:
            try:
                now = _utcnow()
                logger.debug(f"Template scheduler: checking for due templates at {now}")
                
                # Templates due to run now
                due = (db.session.query(ConversionJob)
                       .filter(ConversionJob.recording_id.is_(None))
                       .filter(ConversionJob.status.in_(['pending', 'scheduled']))
                       .filter(ConversionJob.schedule_type.in_(['scheduled','daily','weekly','custom']))
                       .filter(ConversionJob.scheduled_at.isnot(None))
                       .filter(ConversionJob.scheduled_at <= now)
                       .order_by(ConversionJob.id.asc())
                       .all())
                
                logger.info(f"Template scheduler: found {len(due)} due templates")
                
                # Log connection pool status every 10 minutes (every 20th iteration at 30s intervals)
                if hasattr(template_scheduler_loop, '_iteration_count'):
                    template_scheduler_loop._iteration_count += 1
                else:
                    template_scheduler_loop._iteration_count = 1
                
                if template_scheduler_loop._iteration_count % 20 == 0:  # Every 10 minutes
                    log_connection_pool_status()
                for tpl in due:
                    logger.info(f"Template {tpl.id}: type={tpl.schedule_type}, scheduled_at={tpl.scheduled_at}, status={tpl.status}")

                for tpl in due:
                    # Pick recordings that look "ready to convert"
                    ready_query = (
                        db.session.query(Recording)
                        .filter(Recording.status.in_(['completed', 'stopped']))
                    )

                    ready = ready_query.all()
                    logger.info(f"Template {tpl.id}: found {len(ready)} completed recordings available for conversion")
                    created = 0
                    for rec in ready:
                        # Skip if there's already an active/completed job for this recording
                        exists = (db.session.query(ConversionJob)
                                  .filter(ConversionJob.recording_id == rec.id)
                                  .filter(ConversionJob.status.in_(
                                      ['pending','converting','completed']))
                                  .first())
                        if exists:
                            continue

                        job = ConversionJob(
                            recording_id=rec.id,
                            status='pending',
                            schedule_type='immediate',        # run now; worker will pick up
                            scheduled_at=None,
                            custom_filename=tpl.custom_filename or '',
                            delete_original=tpl.delete_original or False,
                        )
                        db.session.add(job)
                        created += 1

                    # Advance or complete the template
                    if tpl.schedule_type == 'scheduled':
                        tpl.status = 'completed'
                    elif tpl.schedule_type == 'daily':
                        tpl.scheduled_at = tpl.scheduled_at + timedelta(days=1)
                        tpl.status = 'pending'
                    elif tpl.schedule_type == 'weekly':
                        tpl.scheduled_at = tpl.scheduled_at + timedelta(weeks=1)
                        tpl.status = 'pending'

                    db.session.commit()
                    logger.info(f"Template {tpl.id}: spawned {created} conversion jobs")
                    if created == 0:
                        logger.info(f"Template {tpl.id}: no jobs created - all recordings may already have active conversion jobs")

                # Compute next due time for adaptive sleep
                next_due = (db.session.query(ConversionJob.scheduled_at)
                           .filter(ConversionJob.recording_id.is_(None))
                           .filter(ConversionJob.status.in_(['pending', 'scheduled']))
                           .filter(ConversionJob.schedule_type.in_(['scheduled','daily','weekly','custom']))
                           .filter(ConversionJob.scheduled_at.isnot(None))
                           .order_by(ConversionJob.scheduled_at.asc())
                           .first())

                sleep_for = 30  # upper bound
                if next_due and next_due[0]:
                    now = _utcnow()
                    delta = (next_due[0] - now).total_seconds()
                    # If next run is sooner than 30s, wake up earlier; never spin faster than 1s
                    sleep_for = max(1, min(30, delta if delta > 0 else 1))

                logger.info(f"Template scheduler: sleeping {sleep_for:.0f}s")
                time.sleep(sleep_for)

            except Exception as e:
                logger.exception(f"Template scheduler error: {e}")
                time.sleep(5)  # Wait 5 seconds on error
            finally:
                # Ensure no connection remains checked-out between iterations
                try:
                    db.session.remove()
                except Exception as se:
                    logger.warning(f"Scheduler: session cleanup error: {se}")

def conversion_worker_loop():
    """Run in a daemon thread with an active Flask app context."""
    with app.app_context():
        logger.info("Conversion worker: started (app context bound)")
        while True:
            session = WorkerSession()  # Create fresh session for each iteration
            try:
                # Get eligible jobs using the proper filter
                jobs = _eligible_conversion_jobs(session)
                
                if not jobs:
                    time.sleep(1)
                    continue
                
                job = jobs[0]  # Take the first eligible job
                
                try:
                    # Re-fetch an attached instance to ensure we work with a fresh, attached object
                    job = session.get(ConversionJob, job.id)
                    
                    # Enhanced worker logging
                    conversion_logger.info(f"🔄 WORKER: Starting conversion job {job.id}")
                    if job.recording_id:
                        rec = session.get(Recording, job.recording_id)
                        if rec:
                            conversion_logger.info(f"🔄 WORKER: Job {job.id} converting recording {rec.id} ({rec.filename})")
                    
                    _start_job(job, session)
                    try:
                        _run_ffmpeg_conversion(job, session)
                        _finalize_job(job, session, ok=True, done_text=f"Completed successfully")
                        conversion_logger.info(f"✅ WORKER: Successfully completed conversion job {job.id}")
                    except Exception as e:
                        _update_job_progress(job, f"Error: {e}", session, force=True)
                        _finalize_job(job, session, ok=False)
                        conversion_logger.error(f"❌ WORKER: Job {job.id} failed with error: {e}")
                        raise
                except Exception as e:
                    app.logger.exception(f"Error processing job {job.id}: {e}")
                    conversion_logger.error(f"❌ WORKER: Critical error processing job {job.id}: {e}")
                
            except Exception as e:
                app.logger.exception("Conversion worker loop error")
                time.sleep(1)
            finally:
                # Clean up session for this iteration
                session.close()
                WorkerSession.remove()

def get_recording_status(recording):
    """Determine the actual status of a recording based on file existence and extension"""
    if not recording.filename:
        return 'unknown'
    
    download_path = get_download_path()
    ts_path = os.path.join(download_path, f"{recording.filename}.ts")
    part_path = os.path.join(download_path, f"{recording.filename}.part")
    
    # Check if file exists and determine status
    if os.path.exists(ts_path):
        return 'completed'
    elif os.path.exists(part_path):
        return 'failed'
    else:
        return 'deleted'

def is_recording_convertible(recording):
    """Check if a recording can be converted (completed .ts file exists and status is eligible)"""
    if not recording.filename:
        return False
    
    # Only convertible if status indicates a finalized recording
    if recording.status not in ['completed', 'stopped']:
        return False
    
    download_path = get_download_path()
    ts_path = os.path.join(download_path, f"{recording.filename}.ts")
    part_path = os.path.join(download_path, f"{recording.filename}.part")
    
    # Must have a .ts file and no .part file (indicates finalized recording)
    return os.path.exists(ts_path) and not os.path.exists(part_path)

def safe_delete_recording_file(filename):
    """Safely delete recording files (.ts and .part)"""
    download_path = get_download_path()
    ts_path = os.path.join(download_path, f"{filename}.ts")
    part_path = os.path.join(download_path, f"{filename}.part")
    
    deleted_files = []
    try:
        if os.path.exists(ts_path):
            os.remove(ts_path)
            deleted_files.append(f"{filename}.ts")
        if os.path.exists(part_path):
            os.remove(part_path)
            deleted_files.append(f"{filename}.part")
        return True, deleted_files
    except Exception as e:
        logger.error(f"Error deleting recording files for {filename}: {e}")
        return False, deleted_files

def ensure_download_directory():
    """Ensure the download directory exists"""
    download_path = get_download_path()
    os.makedirs(download_path, exist_ok=True)
    return download_path

def ensure_converted_directory():
    """Ensure the converted directory exists"""
    converted_path = get_converted_path()
    os.makedirs(converted_path, exist_ok=True)
    return converted_path

def make_filename(streamer):
    """Create a filename for a streamer"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    return f"{streamer.twitch_name} - {timestamp}"

def _normalize_twitch_token(raw: str) -> str:
    """Accept 'oauth:abcd', 'OAuth abcd', or bare token -> return 'abcd'"""
    if not raw:
        return ""
    s = raw.strip()
    if s.lower().startswith("oauth:"):
        s = s.split(":", 1)[1].strip()
    if s.lower().startswith("oauth "):
        s = s.split(" ", 1)[1].strip()
    return s

def build_twitch_cli_cmd(username: str, ts_out_path: str, auth: 'TwitchAuth'):
    """
    Build a Streamlink CLI compatible with v7.1.3 (your image):
      • Twitch auth via header + cookie
      • Version-safe retry/timeout flags
      • Never include unsupported flags (we also sanitize Extra Flags)
    """
    cmd = ["streamlink"]
    cmd += ["--loglevel", "debug" if STREAMLINK_DEBUG else "info"]

    # Auth (per Streamlink Twitch plugin docs)
    if auth:
        tok = _normalize_twitch_token(auth.oauth_token or "")
        if tok:
            cmd += [f"--twitch-api-header=Authorization=OAuth {tok}"]
            cmd += ["--http-cookie", f"auth-token={tok}"]
        if auth.client_id:
            cmd += ["--http-header", f"Client-ID={auth.client_id}"]

    # Make Streamlink itself more resilient
    cmd += [
        "--retry-open", "999999",
        "--retry-streams", "999999",
        "--stream-segment-attempts", "10",
        "--stream-segment-timeout", "20",
        "--hls-segment-threads", "1",
        "--twitch-disable-ads",
    ]
    
    # Conditionally add --hls-live-restart if enabled (can cause indefinite reconnection)
    if auth and auth.enable_hls_live_restart:
        cmd.append("--hls-live-restart")

    # Sanitize user-specified extra flags: drop known-unsupported/deprecated ones on 7.1.3
    DISALLOWED = {"--retry-delay", "--hls-timeout", "--hls-segment-timeout"}
    if auth and (auth.extra_flags or "").strip():
        parts = [p for p in auth.extra_flags.strip().split() if p]
        safe = []
        skip_next = False
        for i, p in enumerate(parts):
            if skip_next:
                skip_next = False
                continue
            if p in DISALLOWED:
                # most disallowed flags expect a value next; drop it too
                skip_next = True
                continue
            safe.append(p)
        if safe:
            cmd += safe

    # Quality + output
    cmd += [f"https://twitch.tv/{username}", "best", "-o", ts_out_path]
    return cmd

def looks_like_twitch(streamer) -> bool:
    """Return True if this streamer should use the Twitch CLI path."""
    try:
        plat = (getattr(streamer, "platform", None) or "").lower()
    except Exception:
        plat = ""
    name = (getattr(streamer, "twitch_name", None) or getattr(streamer, "username", None) or "").lower()
    url  = (getattr(streamer, "url", None) or getattr(streamer, "source_url", None) or "")
    return "twitch" in plat or "twitch.tv" in url.lower() or bool(name)

def build_streamlink_cmd(streamer, output_path):
    """Build the streamlink command for a streamer"""
    quality = streamer.quality or 'best'
    return [
        'streamlink',
        f'twitch.tv/{streamer.twitch_name}',
        quality,
        '-o', output_path,
        '--force'
    ]

def _recording_paths(recording):
    """Get the file paths for a recording"""
    download_path = get_download_path()
    base = os.path.join(download_path, recording.filename)
    return f"{base}.ts", f"{base}.part"

def start_recording_for_streamer(streamer: Streamer):
    """Start a recording for a specific streamer (simplified)"""
    recording_manager = get_recording_manager()
    recording_id = recording_manager.start_recording(streamer.id)
    return recording_id


# Old record_stream function removed - now using RecordingManager
def record_stream(streamer_id: int):
    """Start recording for a streamer (simplified)"""
    recording_manager = get_recording_manager()
    recording_id = recording_manager.start_recording(streamer_id)
    if recording_id:
        logger.info(f"Started recording {recording_id} for streamer {streamer_id}")
    else:
        logger.info(f"Recording already active for streamer {streamer_id}")


# Routes
@app.route('/')
def index():
    """Main dashboard"""
    # Ensure database is migrated before querying
    ensure_database_migrated()
    
    streamers = Streamer.query.all()
    recordings = Recording.query.order_by(Recording.started_at.desc()).limit(10).all()
    
    # Get current status for each streamer
    streamer_status = {}
    for streamer in streamers:
        config = AppConfig(streamer)
        twitch_manager = TwitchManager(config)
        try:
            status, title = twitch_manager.check_user(streamer.twitch_name)
            streamer_status[streamer.id] = {
                'status': status.name if status else 'OFFLINE',
                'title': title
            }
        except Exception as e:
            streamer_status[streamer.id] = {
                'status': 'ERROR',
                'title': str(e)
            }
    
    return render_template('index.html', 
                         streamers=streamers, 
                         recordings=recordings,
                         streamer_status=streamer_status)

@app.route('/favicon.ico')
def favicon_ico():
    """Serve favicon.ico (browser default)"""
    logger.info("Favicon.ico requested")
    try:
        response = send_from_directory('.', 'favicon.png')
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as e:
        logger.error(f"Error serving favicon.ico: {e}")
        return '', 404

@app.route('/favicon.png')
def favicon():
    """Serve favicon"""
    logger.info("Favicon.png requested")
    try:
        # Check if file exists
        import os
        favicon_path = os.path.join('.', 'favicon.png')
        logger.info(f"Favicon path: {favicon_path}")
        logger.info(f"File exists: {os.path.exists(favicon_path)}")
        response = send_from_directory('.', 'favicon.png')
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as e:
        logger.error(f"Error serving favicon.png: {e}")
        return '', 404

@app.route('/test-favicon')
def test_favicon():
    """Test route to check favicon file"""
    import os
    favicon_path = os.path.join('.', 'favicon.png')
    return jsonify({
        'favicon_path': favicon_path,
        'file_exists': os.path.exists(favicon_path),
        'current_dir': os.getcwd(),
        'files_in_dir': os.listdir('.')
    })

@app.route('/api/streamers', methods=['GET'])
@cleanup_session
def get_streamers():
    """API endpoint to get all streamers"""
    logger.info("GET /api/streamers called")
    try:
        streamers = Streamer.query.order_by(Streamer.id).all()
        logger.info(f"Found {len(streamers)} streamers in database")
        
        streamers_data = []
        for s in streamers:
            streamer_data = {
                'id': s.id,
                'username': s.username,
                'twitch_name': s.twitch_name,
                'quality': s.quality,
                'timer': s.timer,
                'is_active': s.is_active,
                'created_at': s.created_at.isoformat(),
                'updated_at': s.updated_at.isoformat()
            }
            streamers_data.append(streamer_data)
            logger.info(f"Streamer: {s.username} (Twitch: {s.twitch_name}) - Active: {s.is_active}")
        
        logger.info(f"Returning {len(streamers_data)} streamers")
        return jsonify(streamers_data)
    except Exception as e:
        logger.error(f"Error in get_streamers: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/streamers', methods=['POST'])
@cleanup_session
def add_streamer():
    """API endpoint to add a new streamer"""
    data = request.get_json()
    
    # Validate required fields
    if not data.get('username') or not data.get('twitch_name'):
        return jsonify({'error': 'Display Name and Twitch name are required'}), 400
    
    # Check if streamer already exists (check both username and twitch_name)
    existing_username = Streamer.query.filter_by(username=data['username']).first()
    existing_twitch = Streamer.query.filter_by(twitch_name=data['twitch_name']).first()
    if existing_username or existing_twitch:
        return jsonify({'error': 'Streamer already exists (check Display Name and Twitch name)'}), 400
    
    # Create new streamer
    streamer = Streamer(
        username=data['username'],
        twitch_name=data['twitch_name'],
        quality=data.get('quality', 'best'),
        timer=data.get('timer', 360),
        is_active=data.get('is_active', True)
    )
    
    db.session.add(streamer)
    db.session.commit()
    
    # Start monitoring if active
    if streamer.is_active:
        try:
            monitor = get_stream_monitor()
            monitor.start_monitoring(streamer.id)
        except Exception as e:
            logger.error(f"Error starting monitoring for streamer {streamer.id}: {e}")
    
    return jsonify({'message': 'Streamer added successfully', 'id': streamer.id}), 201

@app.route('/api/streamers/<int:streamer_id>', methods=['PUT'])
@cleanup_session
def update_streamer(streamer_id):
    """API endpoint to update a streamer"""
    streamer = Streamer.query.get_or_404(streamer_id)
    data = request.get_json()
    
    # Update fields
    if 'username' in data:
        streamer.username = data['username']
    if 'twitch_name' in data:
        streamer.twitch_name = data['twitch_name']
    if 'quality' in data:
        streamer.quality = data['quality']
    if 'timer' in data:
        streamer.timer = data['timer']
    if 'is_active' in data:
        streamer.is_active = data['is_active']
    
    # Store old active state
    was_active = streamer.is_active
    
    streamer.updated_at = datetime.utcnow()
    db.session.commit()
    
    # Handle monitoring state changes
    try:
        monitor = get_stream_monitor()
        recording_manager = get_recording_manager()
        
        if was_active and not streamer.is_active:
            # Stop monitoring and recording if deactivated
            monitor.stop_monitoring(streamer_id)
            recording_manager.stop_recording_by_streamer(streamer_id)
        elif not was_active and streamer.is_active:
            # Start monitoring if activated
            monitor.start_monitoring(streamer_id)
        elif streamer.is_active:
            # Restart monitoring if timer changed (optional optimization)
            monitor.stop_monitoring(streamer_id)
            monitor.start_monitoring(streamer_id)
    except Exception as e:
        logger.error(f"Error handling monitoring for streamer {streamer_id}: {e}")
    
    return jsonify({'message': 'Streamer updated successfully'})

@app.route('/api/streamers/<int:streamer_id>', methods=['DELETE'])
@cleanup_session
def delete_streamer(streamer_id):
    """API endpoint to delete a streamer"""
    streamer = Streamer.query.get_or_404(streamer_id)
    
    # Stop monitoring and any active recording
    try:
        monitor = get_stream_monitor()
        recording_manager = get_recording_manager()
        
        monitor.stop_monitoring(streamer_id)
        recording_manager.stop_recording_by_streamer(streamer_id)
        
        logger.info(f"Stopped monitoring and recording for streamer {streamer_id} before deletion")
    except Exception as e:
        logger.error(f"Error stopping monitoring/recording for streamer {streamer_id}: {e}")
    
    db.session.delete(streamer)
    db.session.commit()
    
    return jsonify({'message': 'Streamer deleted successfully'})

@app.route('/api/monitoring-status')
@cleanup_session
def get_monitoring_status():
    """API endpoint to get monitoring status for all streamers"""
    try:
        monitor = get_stream_monitor()
        status = monitor.get_monitoring_status()
        
        return jsonify({
            'monitoring_status': status,
            'total_monitored': len([s for s in status.values() if s])
        })
    except Exception as e:
        logger.error(f"Error getting monitoring status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/streamers/<int:streamer_id>/force-record', methods=['POST'])
@cleanup_session  
def force_record_streamer(streamer_id):
    """Force start recording for a streamer (for testing)"""
    try:
        recording_manager = get_recording_manager()
        recording_id = recording_manager.start_recording(streamer_id)
        
        if recording_id:
            return jsonify({'message': f'Recording {recording_id} started', 'recording_id': recording_id})
        else:
            return jsonify({'error': 'Failed to start recording or already recording'}), 400
            
    except Exception as e:
        logger.error(f"Error force starting recording for streamer {streamer_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug/test-monitor')
def test_monitor():
    """Test if monitoring components are working"""
    try:
        # Test if modules can be imported
        from recording_manager import get_recording_manager
        from stream_monitor import get_stream_monitor
        
        # Test if they're initialized
        recording_manager = get_recording_manager()
        stream_monitor = get_stream_monitor()
        
        # Get current state
        active_recordings = recording_manager.get_active_recordings()
        monitoring_status = stream_monitor.get_monitoring_status()
        
        # Test manual start of monitoring
        with app.app_context():
            streamers = Streamer.query.filter_by(is_active=True).all()
            for streamer in streamers:
                stream_monitor.start_monitoring(streamer.id)
        
        return jsonify({
            'success': True,
            'active_recordings': len(active_recordings),
            'monitoring_threads_before': len(monitoring_status),
            'active_streamers_in_db': len(streamers),
            'manually_started_monitoring': True,
            'streamers': [{'id': s.id, 'name': s.username, 'twitch': s.twitch_name} for s in streamers]
        })
        
    except ImportError as e:
        return jsonify({
            'error': f'Import error: {str(e)}',
            'error_type': 'ImportError'
        })
    except RuntimeError as e:
        return jsonify({
            'error': f'Runtime error: {str(e)}',
            'error_type': 'RuntimeError' 
        })
    except Exception as e:
        return jsonify({
            'error': f'Unexpected error: {str(e)}',
            'error_type': type(e).__name__
        })

@app.route('/api/debug/monitor-detail/<int:streamer_id>')
def debug_monitor_detail(streamer_id):
    """Debug what the monitor is seeing for a specific streamer"""
    try:
        from recording_manager import get_recording_manager
        from stream_monitor import get_stream_monitor
        
        # Get streamer info
        streamer = Streamer.query.get_or_404(streamer_id)
        
        # Test Twitch API directly
        config = AppConfig(streamer)
        twitch_manager = TwitchManager(config)
        status, title = twitch_manager.check_user(streamer.twitch_name)
        
        # Check recording manager state
        recording_manager = get_recording_manager()
        is_recording = recording_manager.is_streamer_recording(streamer_id)
        active_recordings = recording_manager.get_active_recordings()
        
        # Check for existing recordings in database
        db_recordings = Recording.query.filter_by(
            streamer_id=streamer_id,
            status='recording'
        ).all()
        
        return jsonify({
            'streamer': {
                'id': streamer.id,
                'username': streamer.username,
                'twitch_name': streamer.twitch_name,
                'is_active': streamer.is_active,
                'timer': streamer.timer
            },
            'twitch_status': {
                'status': status.name if status else 'ERROR',
                'title': title,
                'is_online': status.name == 'ONLINE' if status else False
            },
            'recording_state': {
                'is_recording_per_manager': is_recording,
                'active_recordings_count': len(active_recordings),
                'db_recordings_count': len(db_recordings),
                'db_recordings': [{'id': r.id, 'status': r.status, 'started_at': r.started_at.isoformat()} for r in db_recordings]
            },
            'config': {
                'has_client_id': bool(config.client_id),
                'has_client_secret': bool(config.client_secret),
                'has_oauth_token': bool(config.oauth_token)
            }
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/debug/force-check/<int:streamer_id>', methods=['POST'])
def force_check_streamer(streamer_id):
    """Manually trigger the monitoring logic for a streamer"""
    try:
        from recording_manager import get_recording_manager
        
        streamer = Streamer.query.get_or_404(streamer_id)
        
        # Check Twitch status
        config = AppConfig(streamer)
        twitch_manager = TwitchManager(config)
        status, title = twitch_manager.check_user(streamer.twitch_name)
        
        result = {
            'streamer': streamer.twitch_name,
            'status': status.name if status else 'ERROR',
            'title': title,
            'action_taken': 'none'
        }
        
        if status == StreamStatus.ONLINE:
            # Try to start recording
            recording_manager = get_recording_manager()
            
            if not recording_manager.is_streamer_recording(streamer_id):
                recording_id = recording_manager.start_recording(streamer_id)
                if recording_id:
                    result['action_taken'] = f'started_recording_{recording_id}'
                else:
                    result['action_taken'] = 'failed_to_start_recording'
            else:
                result['action_taken'] = 'already_recording'
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/debug/recording-error/<int:streamer_id>', methods=['POST'])
def debug_recording_error(streamer_id):
    """Debug why recording fails to start"""
    try:
        from recording_manager import get_recording_manager
        
        streamer = Streamer.query.get_or_404(streamer_id)
        recording_manager = get_recording_manager()
        
        # Check prerequisites
        download_path = get_download_path()
        
        result = {
            'streamer': {
                'id': streamer.id,
                'username': streamer.username,
                'twitch_name': streamer.twitch_name
            },
            'prerequisites': {
                'download_path': download_path,
                'download_path_exists': os.path.exists(download_path),
                'download_path_writable': os.access(download_path, os.W_OK) if os.path.exists(download_path) else False,
                'streamlink_available': bool(shutil.which('streamlink'))
            },
            'attempt_result': None,
            'error_details': None
        }
        
        # Try to create download directory
        try:
            os.makedirs(download_path, exist_ok=True)
            result['prerequisites']['download_dir_created'] = True
        except Exception as e:
            result['prerequisites']['download_dir_created'] = False
            result['prerequisites']['download_dir_error'] = str(e)
        
        # Try to start recording with detailed error capture
        try:
            recording_id = recording_manager.start_recording(streamer_id)
            if recording_id:
                result['attempt_result'] = f'success_recording_{recording_id}'
            else:
                result['attempt_result'] = 'failed_returned_none'
        except Exception as e:
            result['attempt_result'] = 'exception_thrown'
            result['error_details'] = {
                'error_message': str(e),
                'error_type': type(e).__name__
            }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/debug/test-streamlink', methods=['POST']) 
def test_streamlink():
    """Test if streamlink command works"""
    try:
        # Test basic streamlink
        result = subprocess.run(['streamlink', '--version'], 
                              capture_output=True, text=True, timeout=10)
        
        streamlink_works = result.returncode == 0
        streamlink_output = result.stdout + result.stderr
        
        # Test streamlink with a simple stream
        if streamlink_works:
            test_result = subprocess.run([
                'streamlink', '--loglevel', 'info', 
                'https://twitch.tv/caseoh_', 'best', '--player', 'echo'
            ], capture_output=True, text=True, timeout=30)
            
            stream_test_output = test_result.stdout + test_result.stderr
        else:
            stream_test_output = "Streamlink not available"
            
        return jsonify({
            'streamlink_available': streamlink_works,
            'streamlink_version': streamlink_output.strip(),
            'stream_test_output': stream_test_output.strip()
        })
        
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Streamlink test timed out'})
    except Exception as e:
        return jsonify({'error': str(e), 'error_type': type(e).__name__})

@app.route('/api/debug/recording-manager-deep/<int:streamer_id>', methods=['POST'])
def debug_recording_manager_deep(streamer_id):
    """Deep dive into recording manager internals"""
    try:
        from recording_manager import get_recording_manager
        
        streamer = Streamer.query.get_or_404(streamer_id)
        recording_manager = get_recording_manager()
        
        result = {
            'streamer': {
                'id': streamer.id,
                'username': streamer.username,
                'twitch_name': streamer.twitch_name,
                'is_active': streamer.is_active
            },
            'recording_manager_state': {},
            'step_by_step': {}
        }
        
        # Check if already recording
        is_recording = recording_manager.is_streamer_recording(streamer_id)
        result['recording_manager_state']['is_already_recording'] = is_recording
        
        if is_recording:
            result['step_by_step']['reason'] = 'already_recording'
            return jsonify(result)
        
        # Check Twitch status
        try:
            config = AppConfig(streamer)
            twitch_manager = TwitchManager(config)
            status, title = twitch_manager.check_user(streamer.twitch_name)
            result['step_by_step']['twitch_status'] = status.name if status else 'ERROR'
            result['step_by_step']['twitch_title'] = title
        except Exception as e:
            result['step_by_step']['twitch_error'] = str(e)
            return jsonify(result)
        
        # Check if streamer is online
        if status != StreamStatus.ONLINE:
            result['step_by_step']['reason'] = f'streamer_not_online_{status.name if status else "ERROR"}'
            return jsonify(result)
        
        # Try to start recording with detailed logging
        try:
            # Check internal state before starting
            active_recordings = recording_manager.get_active_recordings()
            result['recording_manager_state']['active_recordings_before'] = len(active_recordings)
            
            # Attempt to start recording
            recording_id = recording_manager.start_recording(streamer_id)
            result['step_by_step']['recording_id_returned'] = recording_id
            
            # Check internal state after starting
            active_recordings_after = recording_manager.get_active_recordings()
            result['recording_manager_state']['active_recordings_after'] = len(active_recordings_after)
            
            # Check database for new recording
            if recording_id:
                db_recording = Recording.query.get(recording_id)
                if db_recording:
                    result['step_by_step']['db_recording_created'] = True
                    result['step_by_step']['db_recording_status'] = db_recording.status
                else:
                    result['step_by_step']['db_recording_created'] = False
                    result['step_by_step']['db_recording_error'] = 'Recording ID returned but not found in database'
            else:
                result['step_by_step']['reason'] = 'start_recording_returned_none'
                
        except Exception as e:
            result['step_by_step']['exception'] = {
                'error_message': str(e),
                'error_type': type(e).__name__
            }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/debug/recording-manager-detail/<int:streamer_id>', methods=['POST'])
def debug_recording_manager_detail(streamer_id):
    """Debug the recording manager's internal logic"""
    try:
        from recording_manager import get_recording_manager
        
        recording_manager = get_recording_manager()
        
        # Check if already recording (this might be the issue)
        is_already_recording = recording_manager.is_streamer_recording(streamer_id)
        
        # Check internal state
        with recording_manager._lock:
            active_recordings = dict(recording_manager._recordings)
            streamer_recordings = dict(recording_manager._streamer_recordings)
        
        # Check database state
        streamer = Streamer.query.get(streamer_id)
        db_active_recordings = Recording.query.filter_by(
            streamer_id=streamer_id,
            status='recording'
        ).all()
        
        result = {
            'streamer_id': streamer_id,
            'manager_state': {
                'is_already_recording': is_already_recording,
                'active_recordings_count': len(active_recordings),
                'active_recordings': {k: {'state': v.state.value, 'filename': v.filename} for k, v in active_recordings.items()},
                'streamer_recordings': streamer_recordings
            },
            'database_state': {
                'active_db_recordings': len(db_active_recordings),
                'recordings': [{'id': r.id, 'status': r.status, 'filename': r.filename} for r in db_active_recordings]
            },
            'streamer_exists': streamer is not None
        }
        
        # Try the recording logic step by step
        if not is_already_recording and streamer:
            try:
                # Create a test recording in the database
                test_recording = Recording(
                    streamer_id=streamer_id,
                    filename=f"test-{streamer.twitch_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                    title="Test Recording",
                    status='recording',
                    started_at=datetime.utcnow()
                )
                
                # Don't commit yet, just test creation
                db.session.add(test_recording)
                db.session.flush()  # Get the ID without committing
                
                result['test_recording'] = {
                    'can_create_recording': True,
                    'test_recording_id': test_recording.id
                }
                
                # Rollback the test
                db.session.rollback()
                
            except Exception as e:
                result['test_recording'] = {
                    'can_create_recording': False,
                    'error': str(e)
                }
                db.session.rollback()
        else:
            result['test_recording'] = {
                'skipped': True,
                'reason': 'already_recording' if is_already_recording else 'no_streamer'
            }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/debug/step-by-step-recording/<int:streamer_id>', methods=['POST'])
def debug_step_by_step_recording(streamer_id):
    """Debug each step of the recording process"""
    try:
        from recording_manager import get_recording_manager
        
        recording_manager = get_recording_manager()
        streamer = Streamer.query.get(streamer_id)
        
        steps = []
        
        # Step 1: Check lock acquisition
        steps.append("Step 1: Acquiring lock...")
        with recording_manager._lock:
            steps.append("Step 1: ✅ Lock acquired")
            
            # Step 2: Check if already recording
            steps.append("Step 2: Checking if already recording...")
            if streamer_id in recording_manager._streamer_recordings:
                existing_id = recording_manager._streamer_recordings[streamer_id]
                existing_info = recording_manager._recordings.get(existing_id)
                if existing_info and not recording_manager._is_recording_stale(existing_info):
                    steps.append(f"Step 2: ❌ Already recording (ID: {existing_id})")
                    return jsonify({'steps': steps, 'result': 'already_recording'})
                else:
                    steps.append(f"Step 2: 🧹 Cleaning up stale recording (ID: {existing_id})")
                    recording_manager._cleanup_recording_unsafe(existing_id)
            
            steps.append("Step 2: ✅ No active recording found")
            
            # Step 3: Create recording in database
            steps.append("Step 3: Creating database record...")
            try:
                with recording_manager.app.app_context():
                    recording = Recording(
                        streamer_id=streamer_id,
                        filename=recording_manager._make_filename(streamer),
                        title="",
                        status='recording',
                        started_at=datetime.utcnow()
                    )
                    recording_manager.db.session.add(recording)
                    recording_manager.db.session.commit()
                    recording_id = recording.id
                    steps.append(f"Step 3: ✅ Database record created (ID: {recording_id})")
            except Exception as e:
                steps.append(f"Step 3: ❌ Database error: {str(e)}")
                return jsonify({'steps': steps, 'result': 'database_error', 'error': str(e)})
            
            # Step 4: Create RecordingInfo object
            steps.append("Step 4: Creating RecordingInfo object...")
            try:
                from recording_manager import RecordingInfo, RecordingState
                info = RecordingInfo(
                    id=recording_id,
                    streamer_id=streamer_id,
                    state=RecordingState.STARTING,
                    started_at=datetime.utcnow(),
                    stop_event=threading.Event(),
                    filename=recording.filename
                )
                steps.append("Step 4: ✅ RecordingInfo created")
            except Exception as e:
                steps.append(f"Step 4: ❌ RecordingInfo error: {str(e)}")
                return jsonify({'steps': steps, 'result': 'recording_info_error', 'error': str(e)})
            
            # Step 5: Create and start thread
            steps.append("Step 5: Creating recording thread...")
            try:
                thread = threading.Thread(
                    target=recording_manager._recording_worker,
                    args=(info,),
                    name=f"recording-{recording_id}",
                    daemon=True
                )
                info.thread = thread
                steps.append("Step 5: ✅ Thread created")
            except Exception as e:
                steps.append(f"Step 5: ❌ Thread creation error: {str(e)}")
                return jsonify({'steps': steps, 'result': 'thread_error', 'error': str(e)})
            
            # Step 6: Register recording
            steps.append("Step 6: Registering recording...")
            try:
                recording_manager._recordings[recording_id] = info
                recording_manager._streamer_recordings[streamer_id] = recording_id
                steps.append("Step 6: ✅ Recording registered")
            except Exception as e:
                steps.append(f"Step 6: ❌ Registration error: {str(e)}")
                return jsonify({'steps': steps, 'result': 'registration_error', 'error': str(e)})
            
            # Step 7: Start thread
            steps.append("Step 7: Starting thread...")
            try:
                thread.start()
                steps.append("Step 7: ✅ Thread started")
                steps.append(f"Step 8: ✅ SUCCESS - Recording {recording_id} started")
                return jsonify({'steps': steps, 'result': 'success', 'recording_id': recording_id})
            except Exception as e:
                steps.append(f"Step 7: ❌ Thread start error: {str(e)}")
                return jsonify({'steps': steps, 'result': 'thread_start_error', 'error': str(e)})
        
    except Exception as e:
        return jsonify({
            'steps': steps + [f"💥 Unexpected error: {str(e)}"],
            'result': 'unexpected_error',
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/debug/recording-status/<int:recording_id>')
def debug_recording_status(recording_id):
    """Check status of a specific recording"""
    try:
        from recording_manager import get_recording_manager
        
        recording_manager = get_recording_manager()
        
        # Check database
        recording = Recording.query.get(recording_id)
        
        # Check recording manager tracking
        with recording_manager._lock:
            is_tracked = recording_id in recording_manager._recordings
            tracking_info = recording_manager._recordings.get(recording_id)
            
        # Check file system
        if recording:
            download_path = get_download_path()
            ts_path = os.path.join(download_path, f"{recording.filename}.ts")
            part_path = os.path.join(download_path, f"{recording.filename}.part")
            
            file_info = {
                'ts_exists': os.path.exists(ts_path),
                'ts_size': os.path.getsize(ts_path) if os.path.exists(ts_path) else 0,
                'part_exists': os.path.exists(part_path),
                'part_size': os.path.getsize(part_path) if os.path.exists(part_path) else 0
            }
        else:
            file_info = {'error': 'recording not found in database'}
        
        return jsonify({
            'recording_id': recording_id,
            'database': {
                'exists': recording is not None,
                'status': recording.status if recording else None,
                'filename': recording.filename if recording else None,
                'started_at': recording.started_at.isoformat() if recording and recording.started_at else None,
                'pid': recording.pid if recording else None
            },
            'tracking': {
                'is_tracked': is_tracked,
                'state': tracking_info.state.value if tracking_info else None,
                'thread_alive': tracking_info.thread.is_alive() if tracking_info and tracking_info.thread else None
            },
            'files': file_info
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'error_type': type(e).__name__
        })

@app.route('/api/recordings', methods=['GET'])
@cleanup_session
def get_recordings():
    """API endpoint to get recordings"""
    # Ensure database is migrated before querying
    ensure_database_migrated()
    
    logger.info("GET /api/recordings called")
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 6, type=int)  # Changed to 6 items per page
        conversion_only = request.args.get('conversion_only', 'false').lower() == 'true'
        include_converted = request.args.get('include_converted', 'false').lower() == 'true'
        
        logger.info(f"Fetching recordings: page={page}, per_page={per_page}, conversion_only={conversion_only}, include_converted={include_converted}")
        
        # Query recordings excluding deleted ones
        recordings_query = Recording.query.filter(Recording.status != 'deleted')
        
        # Only filter out converted recordings if not including them
        if not include_converted:
            # Filter out recordings that have been converted
            # Get all recording IDs that have completed conversion jobs
            converted_recording_ids = db.session.query(ConversionJob.recording_id).filter(
                ConversionJob.status == 'completed'
            ).distinct().all()
            converted_ids = [r[0] for r in converted_recording_ids if r[0] is not None]
            
            if converted_ids:
                recordings_query = recordings_query.filter(~Recording.id.in_(converted_ids))
        
        recordings = recordings_query.order_by(Recording.started_at.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        logger.info(f"Found {recordings.total} total recordings")
        
        recordings_data = []
        for r in recordings.items:
            # Only adjust if not deleted and no live process is tracked
            try:
                recording_manager = get_recording_manager()
                is_recording = recording_manager.is_recording_active(r.id)
            except:
                is_recording = False
            
            if r.status != 'deleted' and not is_recording:
                ts_path, part_path = _recording_paths(r)
                ts_exists = os.path.exists(ts_path)
                part_exists = os.path.exists(part_path)
                new_status = None
                if part_exists and not ts_exists:
                    new_status = 'failed'
                elif ts_exists and not part_exists:
                    new_status = 'completed'
                if new_status and new_status != r.status:
                    r.status = new_status
                    try:
                        r.file_size = os.path.getsize(ts_path if new_status == 'completed' else part_path)
                    except Exception:
                        pass
                    db.session.commit()
            
            # For conversion tab, only include convertible recordings
            if conversion_only:
                convertible = is_recording_convertible(r)
                if not convertible:
                    # Log why recordings aren't convertible for debugging
                    logger.debug(f"Recording {r.id} ({r.filename}) not convertible: status={r.status}")
                    continue
                else:
                    logger.debug(f"Recording {r.id} ({r.filename}) is convertible: status={r.status}")
            
            streamer = Streamer.query.get(r.streamer_id)
            
            # Calculate current duration for active recordings
            if r.status == 'recording' and r.started_at:
                current_duration = int((datetime.utcnow() - r.started_at).total_seconds())
            else:
                current_duration = r.duration or 0
            
            recordings_data.append({
                'id': r.id,
                'streamer_id': r.streamer_id,
                'streamer_name': streamer.username if streamer else 'Unknown',
                'filename': r.filename,
                'title': r.title,
                'game': r.game,
                'status': r.status,
                'started_at': r.started_at.isoformat() + 'Z',
                'ended_at': r.ended_at.isoformat() + 'Z' if r.ended_at else None,
                'file_size': r.file_size,
                'duration': current_duration,
                'pid': r.pid
            })
        
        response_data = {
            'recordings': recordings_data,
            'total': len(recordings_data),  # Use actual count after filtering
            'pages': recordings.pages,
            'current_page': page
        }
        logger.info(f"Returning {len(recordings_data)} recordings")
        return jsonify(response_data)
    except Exception as e:
        logger.error(f"Error in get_recordings: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/status')
@cleanup_session
def get_status():
    """API endpoint to get current system status"""
    active_streamers = Streamer.query.filter_by(is_active=True).count()
    total_recordings = Recording.query.count()
    recent_recordings = Recording.query.filter(
        Recording.started_at >= datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    ).count()
    
    return jsonify({
        'active_streamers': active_streamers,
        'total_recordings': total_recordings,
        'recent_recordings': recent_recordings,
        'download_path': os.getenv('DOWNLOAD_VOLUME_PATH', get_download_path())
    })

@app.route('/api/disk-space')
def get_disk_space():
    """API endpoint to get disk space information"""
    try:
        import shutil
        download_path = get_download_path()
        
        # Get disk usage for the download directory
        total, used, free = shutil.disk_usage(download_path)
        available = free
        
        return jsonify({
            'total': total,
            'used': used,
            'available': available,
            'path': download_path
        })
    except Exception as e:
        logger.error(f"Error getting disk space: {e}")
        return jsonify({'error': 'Failed to get disk space information'}), 500

@app.route('/api/streamers/<int:streamer_id>/check')
def check_streamer_status(streamer_id):
    """API endpoint to check current status of a specific streamer"""
    streamer = Streamer.query.get_or_404(streamer_id)
    config = AppConfig(streamer)
    twitch_manager = TwitchManager(config)
    
    try:
        status, title = twitch_manager.check_user(streamer.twitch_name)
        return jsonify({
            'streamer_id': streamer_id,
            'status': status.name if status else 'OFFLINE',
            'title': title
        })
    except Exception as e:
        return jsonify({
            'streamer_id': streamer_id,
            'status': 'ERROR',
            'title': str(e)
        }), 500



@app.route('/api/active-recordings')
@cleanup_session
def get_active_recordings():
    """API endpoint to get currently active recordings"""
    # Ensure database is migrated before querying
    ensure_database_migrated()
    
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 6, type=int)  # Default 6 per page
        
        logger.info(f"Fetching active recordings: page={page}, per_page={per_page}")
        
        # Use new recording manager to get active recordings
        recording_manager = get_recording_manager()
        active_recordings_info = recording_manager.get_active_recordings()
        
        # Convert to list and sort by started_at
        active_recordings_list = list(active_recordings_info.values())
        active_recordings_list.sort(key=lambda x: x.started_at or datetime.min, reverse=True)
        
        # Paginate manually
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_recordings = active_recordings_list[start_idx:end_idx]
        
        recordings_data = []
        for info in paginated_recordings:
            # Get recording from database for additional info
            recording = Recording.query.get(info.id)
            streamer = Streamer.query.get(info.streamer_id) if recording else None
            
            if not recording or not streamer:
                continue
            
            # Calculate current duration for active recordings
            if info.started_at:
                current_duration = int((datetime.utcnow() - info.started_at).total_seconds())
            else:
                current_duration = 0
            
            recordings_data.append({
                'id': recording.id,
                'streamer_id': recording.streamer_id,
                'streamer_name': streamer.username if streamer else 'Unknown',
                'filename': recording.filename,
                'title': recording.title,
                'game': recording.game,
                'status': recording.status,
                'started_at': recording.started_at.isoformat() + 'Z',
                'file_size': recording.file_size,
                'duration': current_duration
            })
        
        # Calculate pagination info
        total = len(active_recordings_list)
        pages = (total + per_page - 1) // per_page  # Ceiling division
        
        logger.info(f"Returning {len(recordings_data)} active recordings")
        return jsonify({
            'recordings': recordings_data,
            'total': total,
            'pages': pages,
            'current_page': page,
            'per_page': per_page
        })
    except Exception as e:
        logger.error(f"Error in get_active_recordings: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/recordings/<int:recording_id>/stop', methods=['POST'])
def stop_recording(recording_id):
    """API endpoint to stop a recording"""
    recording = Recording.query.get_or_404(recording_id)
    
    if recording.status != 'recording':
        return jsonify({'error': 'Recording is not currently active'}), 400
    
    try:
        # Use new recording manager to stop recording
        recording_manager = get_recording_manager()
        success = recording_manager.stop_recording(recording_id)
        
        if success:
            logger.info(f"Stopped recording {recording_id}")
            return jsonify({'message': 'Recording stopped successfully'})
        else:
            logger.warning(f"Failed to stop recording {recording_id}")
            return jsonify({'error': 'Failed to stop recording'}), 500
    except Exception as e:
        logger.error(f"Error stopping recording {recording_id}: {e}")
        return jsonify({'error': 'Failed to stop recording'}), 500

@app.route('/api/test/create-sample-recording', methods=['POST'])
def create_sample_recording():
    """Test endpoint to create a sample recording for testing the conversion tab"""
    try:
        # Get the first streamer
        streamer = Streamer.query.first()
        if not streamer:
            return jsonify({'error': 'No streamers found'}), 400
        
        # Create a sample recording
        recording = Recording(
            streamer_id=streamer.id,
            filename=f"{streamer.twitch_name}_20240820_sample",
            title="Sample Recording for Testing",
            game="Test Game",
            status='completed',
            started_at=datetime.utcnow() - timedelta(hours=1),
            ended_at=datetime.utcnow(),
            duration=3600,  # 1 hour
            file_size=1024*1024*100  # 100MB
        )
        
        db.session.add(recording)
        db.session.commit()
        
        logger.info(f"Created sample recording: {recording.filename}")
        return jsonify({'message': 'Sample recording created successfully', 'recording_id': recording.id})
    except Exception as e:
        logger.error(f"Error creating sample recording: {e}")
        return jsonify({'error': 'Failed to create sample recording'}), 500

@app.route('/api/conversion-settings', methods=['GET'])
def get_conversion_settings():
    """API endpoint to get conversion settings"""
    logger.info("GET /api/conversion-settings called")
    try:
        settings = ConversionSettings.query.first()
        logger.info(f"Found settings: {settings}")
        if not settings:
            logger.info("No settings found, creating default")
            settings = ConversionSettings()
            db.session.add(settings)
            db.session.commit()
            logger.info("Default settings created")
        
        response_data = {
            'volume_path': get_download_volume_path(),
            'output_volume_path': get_converted_volume_path(),
            'naming_scheme': settings.naming_scheme,
            'custom_filename_template': settings.custom_filename_template or '',
            'delete_original_after_conversion': settings.delete_original_after_conversion,
            'ffmpeg_preset': settings.ffmpeg_preset,  # NEW
            'available_presets': [
                {"key": k, "label": v["label"], "desc": v["desc"]}
                for k, v in FFMPEG_PRESETS.items()
            ],  # NEW
            # Watchdog settings
            'watchdog_offline_grace_s': settings.watchdog_offline_grace_s,
            'watchdog_stall_timeout_s': settings.watchdog_stall_timeout_s,
            'watchdog_max_duration_s': settings.watchdog_max_duration_s,
        }
        logger.info(f"Returning settings: {response_data}")
        return jsonify(response_data)
    except Exception as e:
        logger.error(f"Error in get_conversion_settings: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/conversion-settings', methods=['POST'])
def save_conversion_settings():
    """API endpoint to save conversion settings"""
    data = request.get_json()
    
    settings = ConversionSettings.query.first()
    if not settings:
        settings = ConversionSettings()
        db.session.add(settings)
    
    # Don't save volume paths as they're now environment variables
    settings.custom_filename_template = data.get('custom_filename_template', '')
    settings.delete_original_after_conversion = data.get('delete_original_after_conversion', False)
    
    # NEW: preset validation and saving
    if "ffmpeg_preset" in data:
        key = data["ffmpeg_preset"]
        if key not in FFMPEG_PRESETS:
            return jsonify({"error": "Invalid ffmpeg_preset"}), 400
        settings.ffmpeg_preset = key
    
    # Watchdog settings validation and saving
    if "watchdog_offline_grace_s" in data:
        grace = int(data["watchdog_offline_grace_s"])
        if grace < 30 or grace > 600:  # 30s to 10 minutes
            return jsonify({"error": "Offline grace period must be between 30 and 600 seconds"}), 400
        settings.watchdog_offline_grace_s = grace
    
    if "watchdog_stall_timeout_s" in data:
        stall = int(data["watchdog_stall_timeout_s"])
        if stall < 60 or stall > 1800:  # 1 minute to 30 minutes
            return jsonify({"error": "Stall timeout must be between 60 and 1800 seconds"}), 400
        settings.watchdog_stall_timeout_s = stall
    
    if "watchdog_max_duration_s" in data:
        max_dur = int(data["watchdog_max_duration_s"])
        if max_dur < 3600 or max_dur > 24*3600:  # 1 hour to 24 hours
            return jsonify({"error": "Max duration must be between 3600 and 86400 seconds"}), 400
        settings.watchdog_max_duration_s = max_dur
    
    settings.updated_at = datetime.utcnow()
    
    db.session.commit()
    
    return jsonify({'message': 'Settings saved successfully'})

@app.route('/api/recordings/delete', methods=['POST'])
def delete_recording():
    """API endpoint to delete a recording file and mark as deleted in DB"""
    try:
        data = request.get_json()
        filename = data.get('filename')
        
        if not filename:
            return jsonify({'error': 'Filename is required'}), 400
        
        # Find the recording by filename
        recording = Recording.query.filter_by(filename=filename).first()
        if not recording:
            return jsonify({'error': 'Recording not found'}), 404
        
        # Delete the actual files
        success, deleted_files = safe_delete_recording_file(filename)
        
        if success:
            # Update recording status to deleted
            recording.status = 'deleted'
            db.session.commit()
            
            logger.info(f"Recording {filename} marked as deleted. Files removed: {deleted_files}")
            return jsonify({
                'message': 'Recording deleted successfully',
                'deleted_files': deleted_files
            })
        else:
            return jsonify({'error': 'Failed to delete recording files'}), 500
            
    except Exception as e:
        logger.error(f"Error deleting recording: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/convert-recordings', methods=['POST'])
def convert_recordings():
    """API endpoint to start conversion of recordings"""
    data = request.get_json()
    recording_ids = data.get('recording_ids', [])
    # Use "immediate" for manual Convert Selected; "scheduled" remains for future runs
    schedule_type = data.get('schedule_type', 'immediate')  # immediate, scheduled, daily, weekly
    scheduled_time = data.get('scheduled_time')  # ISO format string
    custom_filename = data.get('custom_filename', '')  # Custom filename template
    
    # Validate that all recordings are convertible
    for recording_id in recording_ids:
        recording = Recording.query.get(recording_id)
        if not recording:
            return jsonify({'error': f'Recording {recording_id} not found'}), 404
        
        if not is_recording_convertible(recording):
            return jsonify({'error': f'Recording {recording.filename} cannot be converted (not completed or deleted)'}), 400
    
    # Get global delete setting from conversion settings
    settings = ConversionSettings.query.first()
    if not settings:
        settings = ConversionSettings()
        db.session.add(settings)
        db.session.commit()
    
    delete_original = settings.delete_original_after_conversion
    
    # Parse scheduled time if provided
    scheduled_datetime = None
    if scheduled_time:
        try:
            scheduled_datetime = datetime.fromisoformat(scheduled_time.replace('Z', '+00:00'))
            # Normalize to naive UTC for consistent storage
            if scheduled_datetime.tzinfo is not None:
                scheduled_datetime = scheduled_datetime.astimezone(timezone.utc).replace(tzinfo=None)
        except ValueError:
            return jsonify({'error': 'Invalid scheduled time format'}), 400
    
    # For immediate jobs, leave scheduled_at as None (don't set it)
    # For scheduled jobs, ensure we have a scheduled time
    if schedule_type == 'scheduled' and not scheduled_datetime:
        return jsonify({'error': 'Scheduled time is required for scheduled conversions'}), 400
    
    # Debug logging
    logger.info(f"Conversion request - schedule_type: {schedule_type}, scheduled_time: {scheduled_time}, scheduled_datetime: {scheduled_datetime}")
    
    for recording_id in recording_ids:
        # Check if there's an active conversion job (pending or converting)
        existing_job = ConversionJob.query.filter_by(recording_id=recording_id).filter(
            ConversionJob.status.in_(['pending', 'converting'])
        ).first()
        if not existing_job:
            job = ConversionJob(
                recording_id=recording_id,
                schedule_type=schedule_type,
                scheduled_at=scheduled_datetime,
                custom_filename=custom_filename,
                delete_original=delete_original
            )
            db.session.add(job)
    
    db.session.commit()
    
    # Nudge the background worker
    conversion_wakeup.set()
    
    # Message that matches manual vs scheduled
    msg = 'Conversion queued to start now' if schedule_type == 'immediate' else (
        f'Conversion scheduled for {scheduled_datetime}' if scheduled_datetime else 'Conversion scheduled'
    )
    return jsonify({'message': msg})

@app.route('/health')
def health_check():
    """Health check endpoint for Docker"""
    return jsonify({'status': 'healthy'})



@app.route('/api/twitch-auth', methods=['GET'])
def get_twitch_auth():
    ta = TwitchAuth.query.first()
    if not ta:
        return jsonify({'client_id': '', 'client_secret': '', 'oauth_token': '', 'extra_flags': ''})
    # Masked response—only indicate presence, the UI can optionally reveal fully if you prefer
    def mask(s):
        if not s: return ''
        return s[:4] + '•••' + s[-4:] if len(s) > 8 else '•••'
    return jsonify({
        'client_id': ta.client_id or '',
        'client_secret': mask(ta.client_secret),
        'oauth_token': mask(ta.oauth_token),
        'extra_flags': ta.extra_flags or ''
    })

@app.route('/api/twitch-auth', methods=['POST'])
def set_twitch_auth():
    data = request.get_json(force=True) or {}
    client_id = (data.get('client_id') or '').strip()
    client_secret = (data.get('client_secret') or '').strip()
    oauth_token = (data.get('oauth_token') or '').strip()
    extra_flags = (data.get('extra_flags') or '').strip()
    ta = TwitchAuth.query.first()
    if not ta:
        ta = TwitchAuth()
        db.session.add(ta)
    # Update fields only if provided; allow clearing with explicit empty string
    ta.client_id = client_id
    ta.client_secret = client_secret
    ta.oauth_token = oauth_token
    ta.extra_flags = extra_flags
    ta.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify({'message': 'Twitch auth saved'})

@app.route('/api/conversion-progress')
def get_conversion_progress():
    """API endpoint to get conversion progress with pagination"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 6, type=int)  # Default 6 per page
        
        logger.info(f"Fetching conversion progress: page={page}, per_page={per_page}")
        
        # Filter out template jobs (jobs with recording_id = NULL) and only show real conversion jobs
        jobs = ConversionJob.query.filter(ConversionJob.recording_id.isnot(None)).order_by(ConversionJob.id.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        conversions = []
        for job in jobs.items:
            try:
                recording = Recording.query.get(job.recording_id) if job.recording_id else None
                conversions.append({
                    'job_id': job.id,
                    'recording_id': job.recording_id,
                    'filename': recording.filename if recording else 'Template Job',
                    'status': job.status,
                    'progress': job.progress,
                    'output_filename': job.output_filename,
                    'schedule_type': job.schedule_type,
                    'scheduled_at': job.scheduled_at.isoformat() + 'Z' if job.scheduled_at else None,
                    'started_at': job.started_at.isoformat() + 'Z' if job.started_at else None,
                    'completed_at': job.completed_at.isoformat() + 'Z' if job.completed_at else None,
                    'custom_filename': job.custom_filename,
                    'delete_original': job.delete_original
                })
            except Exception as e:
                logger.error(f"Error processing conversion job {job.id}: {e}")
                continue
        
        logger.info(f"Returning {len(conversions)} conversion jobs")
        return jsonify({
            'conversions': conversions,
            'total': jobs.total,
            'pages': jobs.pages,
            'current_page': page,
            'per_page': per_page
        })
    except Exception as e:
        logger.error(f"Error in get_conversion_progress: {e}")
        return jsonify({'error': f'Failed to get conversion progress: {str(e)}'}), 500

@app.route('/api/conversion-jobs/<int:job_id>', methods=['DELETE'])
def cancel_conversion_job(job_id):
    """API endpoint to cancel a conversion job"""
    job = ConversionJob.query.get_or_404(job_id)
    
    if job.status in ['completed', 'failed']:
        return jsonify({'error': 'Cannot cancel completed or failed jobs'}), 400
    
    # Hard-stop fallback: terminate FFmpeg process if still running
    proc_info = conversion_processes.get(job_id)
    if proc_info and proc_info.get("proc"):
        proc = proc_info["proc"]
        if proc.poll() is None:  # Process is still running
            logger.warning(f"Conversion job {job_id} is still running, terminating FFmpeg process {proc.pid}")
            try:
                proc.terminate()
                # Give terminate a moment to work
                time.sleep(2)
                if proc.poll() is None:  # Still running, force kill
                    logger.warning(f"FFmpeg process {proc.pid} didn't terminate, killing it")
                    proc.kill()
            except Exception as e:
                logger.error(f"Error terminating FFmpeg process {proc.pid}: {e}")
    
    db.session.delete(job)
    db.session.commit()
    
    return jsonify({'message': 'Conversion job cancelled successfully'})

@app.route('/api/conversion-jobs/<int:job_id>/remove-from-history', methods=['DELETE'])
def remove_conversion_job_from_history(job_id):
    """API endpoint to remove a conversion job from history (doesn't delete files)"""
    job = ConversionJob.query.get_or_404(job_id)
    
    db.session.delete(job)
    db.session.commit()
    
    return jsonify({'message': 'Conversion job removed from history successfully'})

@app.route('/api/conversion-jobs/<int:job_id>/delete-file', methods=['DELETE'])
def delete_converted_file(job_id):
    """API endpoint to delete the converted file for a conversion job"""
    job = ConversionJob.query.get_or_404(job_id)
    
    if not job.output_filename:
        return jsonify({'error': 'No output file associated with this job'}), 400
    
    # Get the converted file path
    converted_path = get_converted_path()
    output_file = os.path.join(converted_path, job.output_filename)
    
    try:
        if os.path.exists(output_file):
            os.remove(output_file)
            # Clear the output_filename since the file no longer exists
            job.output_filename = None
            db.session.commit()
            logger.info(f"Deleted converted file: {output_file}")
            return jsonify({'message': 'Converted file deleted successfully'})
        else:
            # File doesn't exist, but clear the output_filename anyway
            job.output_filename = None
            db.session.commit()
            return jsonify({'error': 'Converted file not found'}), 404
    except Exception as e:
        logger.error(f"Error deleting converted file {output_file}: {e}")
        return jsonify({'error': f'Failed to delete converted file: {str(e)}'}), 500

@app.route('/api/recordings/<int:recording_id>/remove-from-history', methods=['DELETE'])
def remove_recording_from_history(recording_id):
    """API endpoint to remove a recording from history (doesn't delete files)"""
    recording = Recording.query.get_or_404(recording_id)
    
    # Mark as deleted in database but don't delete files
    recording.status = 'deleted'
    db.session.commit()
    
    return jsonify({'message': 'Recording removed from history successfully'})

@app.route('/api/conversion-jobs/<int:job_id>/reschedule', methods=['POST'])
def reschedule_conversion_job(job_id):
    """API endpoint to reschedule a conversion job"""
    job = ConversionJob.query.get_or_404(job_id)
    data = request.get_json()
    
    if job.status in ['completed', 'failed']:
        return jsonify({'error': 'Cannot reschedule completed or failed jobs'}), 400
    
    # Parse new scheduled time
    scheduled_time = data.get('scheduled_time')
    if scheduled_time:
        try:
            job.scheduled_at = datetime.fromisoformat(scheduled_time.replace('Z', '+00:00'))
        except ValueError:
            return jsonify({'error': 'Invalid scheduled time format'}), 400
    
    job.schedule_type = data.get('schedule_type', job.schedule_type)
    job.status = 'pending'  # Reset to pending
    db.session.commit()
    
    return jsonify({'message': 'Conversion job rescheduled successfully'})

@app.route('/api/schedule-template', methods=['POST'])
def create_schedule_template():
    """API endpoint to create a scheduled template for future conversions"""
    try:
        data = request.get_json()
        logger.info(f"Received schedule template data: {data}")
        
        schedule_type = data.get('schedule_type', 'scheduled')
        scheduled_time = data.get('scheduled_time')
        custom_filename = data.get('custom_filename', '')
        
        # Get global delete setting from conversion settings
        settings = ConversionSettings.query.first()
        if not settings:
            settings = ConversionSettings()
            db.session.add(settings)
            db.session.commit()
        
        delete_original = settings.delete_original_after_conversion
        
        # Parse scheduled time if provided
        scheduled_datetime = None
        if scheduled_time:
            try:
                scheduled_datetime = datetime.fromisoformat(scheduled_time.replace('Z', '+00:00'))
                # Normalize to naive UTC for consistent storage
                if scheduled_datetime.tzinfo is not None:
                    scheduled_datetime = scheduled_datetime.astimezone(timezone.utc).replace(tzinfo=None)
                logger.info(f"Parsed scheduled time: {scheduled_datetime}")
            except ValueError as e:
                logger.error(f"Invalid scheduled time format: {scheduled_time}, error: {e}")
                return jsonify({'error': 'Invalid scheduled time format'}), 400
        
        # Create a template job (no specific recording_id)
        template_job = ConversionJob(
            recording_id=None,  # No specific recording for template
            schedule_type=schedule_type,
            scheduled_at=scheduled_datetime,
            custom_filename=custom_filename,
            delete_original=delete_original,
            status='pending'
        )
        
        db.session.add(template_job)
        db.session.commit()
        
        logger.info(f"Created schedule template: {schedule_type} at {scheduled_datetime}, job_id: {template_job.id}")
        return jsonify({'message': 'Schedule template created successfully', 'job_id': template_job.id})
    except Exception as e:
        logger.error(f"Error creating schedule template: {e}")
        db.session.rollback()
        return jsonify({'error': f'Failed to create schedule template: {str(e)}'}), 500

@app.route('/api/schedule-template', methods=['GET'])
def get_schedule_templates():
    """API endpoint to get active schedule templates (not conversion jobs)"""
    try:
        # Get only template jobs (recording_id is None) that are actual schedules
        templates = ConversionJob.query.filter(
            ConversionJob.recording_id.is_(None),  # Template jobs only
            ConversionJob.schedule_type.in_(['daily', 'weekly', 'custom', 'scheduled'])  # Actual schedules only
        ).order_by(ConversionJob.scheduled_at.asc()).all()
        
        templates_data = []
        for template in templates:
            templates_data.append({
                'id': template.id,
                'schedule_type': template.schedule_type,
                'scheduled_at': template.scheduled_at.isoformat() + 'Z' if template.scheduled_at else None,
                'custom_filename': template.custom_filename,
                'status': template.status,
                'next_run': template.scheduled_at.isoformat() + 'Z' if template.scheduled_at else None
            })
        
        logger.info(f"Returning {len(templates_data)} schedule templates")
        return jsonify({'templates': templates_data})
    except Exception as e:
        logger.error(f"Error getting schedule templates: {e}")
        return jsonify({'error': f'Failed to get schedule templates: {str(e)}'}), 500



def convert_ts_to_mp4(input_file, output_file, job):
    """Convert TS file to MP4 using FFmpeg"""
    try:
        import subprocess
        
        # FFmpeg command for TS to MP4 conversion
        # -i: input file
        # -c:v copy: copy video stream without re-encoding (fast)
        # -c:a aac: convert audio to AAC format
        # -y: overwrite output file if it exists
        cmd = [
            'ffmpeg',
            '-i', input_file,
            '-c:v', 'copy',  # Copy video stream (no re-encoding)
            '-c:a', 'aac',   # Convert audio to AAC
            '-y',            # Overwrite output
            output_file
        ]
        
        # Update progress
        job.progress = 'Running FFmpeg conversion...'
        db.session.commit()
        
        # Run FFmpeg command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            # Check if output file was created and has size > 0
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                return True
            else:
                logger.error(f"FFmpeg completed but output file is missing or empty: {output_file}")
                return False
        else:
            logger.error(f"FFmpeg failed with return code {result.returncode}")
            logger.error(f"FFmpeg stderr: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"FFmpeg conversion timed out for {input_file}")
        return False
    except Exception as e:
        logger.error(f"Error running FFmpeg: {e}")
        return False

def build_output_filename(recording, naming_scheme):
    """Build output filename based on naming scheme"""
    streamer = Streamer.query.get(recording.streamer_id)
    streamer_name = streamer.username if streamer else 'unknown'
    
    # Clean title for filename
    safe_title = "".join(c for c in (recording.title or 'untitled') if c.isalnum() or c in (' ', '-', '_')).rstrip()
    
    # Get date from recording start time
    date_str = recording.started_at.strftime('%Y-%m-%d')
    
    if naming_scheme == 'streamer_date_title':
        return f"{streamer_name} - {date_str} - {safe_title}.mp4"
    elif naming_scheme == 'date_streamer_title':
        return f"{date_str} - {streamer_name} - {safe_title}.mp4"
    elif naming_scheme == 'title_date_streamer':
        return f"{safe_title} - {date_str} - {streamer_name}.mp4"
    elif naming_scheme == 'streamer_title_date':
        return f"{streamer_name} - {safe_title} - {date_str}.mp4"
    else:
        return f"{streamer_name} - {date_str} - {safe_title}.mp4"

def build_custom_filename(template, recording):
    """Build custom filename using template variables"""
    streamer = Streamer.query.get(recording.streamer_id)
    streamer_name = streamer.twitch_name if streamer else 'unknown'  # Use twitch_name instead of username
    twitch_name = streamer.twitch_name if streamer else 'unknown'
    
    # Clean title for filename (remove invalid characters)
    safe_title = "".join(c for c in (recording.title or 'untitled') if c.isalnum() or c in (' ', '-', '_')).rstrip()
    # Replace spaces with underscores for better filename compatibility
    safe_title = safe_title.replace(' ', '_')
    
    # Get date and time from recording start time
    date_str = recording.started_at.strftime('%Y-%m-%d')
    time_str = recording.started_at.strftime('%H-%M-%S')
    datetime_str = recording.started_at.strftime('%Y-%m-%d_%H-%M-%S')
    
    # Replace template variables
    filename = template
    filename = filename.replace('{streamer}', streamer_name)
    filename = filename.replace('{twitch_name}', twitch_name)
    filename = filename.replace('{title}', safe_title)
    filename = filename.replace('{date}', date_str)
    filename = filename.replace('{time}', time_str)
    filename = filename.replace('{datetime}', datetime_str)
    filename = filename.replace('{game}', recording.game or 'unknown')
    
    # Clean up any remaining invalid characters
    filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.', '/', '\\'))
    filename = filename.strip()
    
    # Ensure it ends with .mp4
    if not filename.endswith('.mp4'):
        filename += '.mp4'
    
    return filename

# ---- Enforce session cleanup on routes that missed the decorator ----
_routes_needing_cleanup = [
    # safe to wrap even if they don't use the DB heavily
    'get_disk_space',
    # DB-reading/writing endpoints that were not decorated
    'check_streamer_status',
    'stop_recording',
    'create_sample_recording',
    'get_conversion_settings',
    'save_conversion_settings',
    'delete_recording',
    'convert_recordings',
    'get_conversion_progress',
    'cancel_conversion_job',
    'remove_conversion_job_from_history',
    'delete_converted_file',
    'remove_recording_from_history',
    'reschedule_conversion_job',
    'create_schedule_template',
    'get_schedule_templates',
]
for _name in _routes_needing_cleanup:
    if _name in app.view_functions:
        app.view_functions[_name] = cleanup_session(app.view_functions[_name])

if __name__ == '__main__':
    # Create database tables with retry logic
    max_retries = 3
    retry_delay = 1
    
    logger.info("=== Starting Streamlink Web GUI ===")
    logger.info(f"Database URI: {app.config['SQLALCHEMY_DATABASE_URI']}")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Data directory exists: {os.path.exists(data_dir)}")
    logger.info(f"Data directory writable: {os.access(data_dir, os.W_OK) if os.path.exists(data_dir) else 'N/A'}")
    
    # Debug volume paths
    logger.info(f"DOWNLOAD_VOLUME_PATH: {os.getenv('DOWNLOAD_VOLUME_PATH', 'Not set')}")
    logger.info(f"DATA_VOLUME_PATH: {os.getenv('DATA_VOLUME_PATH', 'Not set')}")
    logger.info(f"CONFIG_VOLUME_PATH: {os.getenv('CONFIG_VOLUME_PATH', 'Not set')}")
    
    # Log streamlink version once for troubleshooting
    try:
        import shutil, subprocess
        if shutil.which("streamlink"):
            v = subprocess.check_output(["streamlink", "--version"], text=True).strip()
            logger.info("Detected %s", v)
    except Exception as e:
        logger.warning("Could not read streamlink --version: %s", e)
    
    # List contents of data directory
    try:
        if os.path.exists(data_dir):
            files = os.listdir(data_dir)
            logger.info(f"Files in data directory: {files}")
        else:
            logger.warning("Data directory does not exist!")
    except Exception as e:
        logger.error(f"Error listing data directory: {e}")
    
    for attempt in range(max_retries):
        try:
            with app.app_context():
                logger.info(f"Attempt {attempt + 1}: Creating database tables...")
                db.create_all()
                # Safe ALTERs if table/columns don't exist (sqlite)
                with db.engine.begin() as conn:
                    conn.execute(db.text("PRAGMA foreign_keys=ON"))
                    # Create twitch_auth table if not present
                    conn.execute(db.text("""
                    CREATE TABLE IF NOT EXISTS twitch_auth (
                        id INTEGER PRIMARY KEY,
                        client_id VARCHAR(128),
                        client_secret VARCHAR(256),
                        oauth_token VARCHAR(512),
                        updated_at DATETIME
                    )
                    """))
                    # Add new columns to recording if missing
                    for ddl in [
                        "ALTER TABLE recording ADD COLUMN pid INTEGER",
                        "ALTER TABLE recording ADD COLUMN session_guid VARCHAR(36)",
                        "ALTER TABLE recording ADD COLUMN status_detail VARCHAR(255)"
                    ]:
                        try: conn.execute(db.text(ddl))
                        except Exception: pass
                    try: conn.execute(db.text("ALTER TABLE twitch_auth ADD COLUMN extra_flags VARCHAR(512)"))
                    except Exception: pass
                    try: conn.execute(db.text("ALTER TABLE twitch_auth ADD COLUMN enable_hls_live_restart BOOLEAN DEFAULT 0"))
                    except Exception: pass
                
                # Check if we need to migrate the database schema
                try:
                    # Try to access the new columns to see if they exist
                    with db.engine.connect() as conn:
                        result = conn.execute(db.text("PRAGMA table_info(conversion_job)"))
                        columns = [row[1] for row in result.fetchall()]
                        
                        # Check if new columns exist
                        new_columns = ['scheduled_at', 'schedule_type', 'custom_filename', 'delete_original']
                        missing_columns = [col for col in new_columns if col not in columns]
                        
                        if missing_columns:
                            logger.info(f"Database migration needed. Missing columns: {missing_columns}")
                            
                            # Add missing columns
                            for col in missing_columns:
                                if col == 'scheduled_at':
                                    conn.execute(db.text("ALTER TABLE conversion_job ADD COLUMN scheduled_at DATETIME"))
                                elif col == 'schedule_type':
                                    conn.execute(db.text("ALTER TABLE conversion_job ADD COLUMN schedule_type VARCHAR(20)"))
                                elif col == 'custom_filename':
                                    conn.execute(db.text("ALTER TABLE conversion_job ADD COLUMN custom_filename VARCHAR(500)"))
                                elif col == 'delete_original':
                                    conn.execute(db.text("ALTER TABLE conversion_job ADD COLUMN delete_original BOOLEAN DEFAULT 0"))
                            
                            conn.commit()
                            logger.info("Database migration completed successfully")
                        else:
                            logger.info("Database schema is up to date")
                        
                        # Check conversion_settings table
                        result = conn.execute(db.text("PRAGMA table_info(conversion_settings)"))
                        settings_columns = [row[1] for row in result.fetchall()]
                        
                        # Check if new columns exist in conversion_settings
                        new_settings_columns = ['output_volume_path', 'custom_filename_template', 'delete_original_after_conversion', 'watchdog_offline_grace_s', 'watchdog_stall_timeout_s', 'watchdog_max_duration_s']
                        missing_settings_columns = [col for col in new_settings_columns if col not in settings_columns]
                        
                        if missing_settings_columns:
                            logger.info(f"Conversion settings migration needed. Missing columns: {missing_settings_columns}")
                            
                            # Add missing columns
                            for col in missing_settings_columns:
                                if col == 'output_volume_path':
                                    conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN output_volume_path VARCHAR(500)"))
                                elif col == 'custom_filename_template':
                                    conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN custom_filename_template VARCHAR(500)"))
                                elif col == 'delete_original_after_conversion':
                                    conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN delete_original_after_conversion BOOLEAN DEFAULT 0"))
                                elif col == 'watchdog_offline_grace_s':
                                    conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN watchdog_offline_grace_s INTEGER DEFAULT 90"))
                                elif col == 'watchdog_stall_timeout_s':
                                    conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN watchdog_stall_timeout_s INTEGER DEFAULT 180"))
                                elif col == 'watchdog_max_duration_s':
                                    conn.execute(db.text("ALTER TABLE conversion_settings ADD COLUMN watchdog_max_duration_s INTEGER DEFAULT 28800"))
                            
                            conn.commit()
                            logger.info("Conversion settings migration completed successfully")
                        else:
                            logger.info("Conversion settings schema is up to date")
                            
                except Exception as migration_error:
                    logger.warning(f"Database migration check failed: {migration_error}")
                    # Continue anyway, the app might still work
                
                logger.info("Database created/updated successfully")
                
                # Check if database file exists and has content
                db_path = f"{data_dir}/streamlink.db"
                if os.path.exists(db_path):
                    file_size = os.path.getsize(db_path)
                    logger.info(f"Database file exists: {db_path}, size: {file_size} bytes")
                else:
                    logger.warning(f"Database file does not exist: {db_path}")
                
                break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database creation failed (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Failed to create database after {max_retries} attempts: {e}")
                # Try multiple fallback locations (in order of preference)
                fallback_paths = ['/app/config/streamlink.db', '/tmp/streamlink.db', './streamlink.db']
                
                for fallback_path in fallback_paths:
                    try:
                        logger.info(f"Trying fallback database location: {fallback_path}")
                        app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{fallback_path}'
                        with app.app_context():
                            db.create_all()
                        logger.info(f"Created database in fallback location: {fallback_path}")
                        break
                    except Exception as fallback_error:
                        logger.warning(f"Fallback location {fallback_path} failed: {fallback_error}")
                        continue
                else:
                    logger.error("All database locations failed. Exiting.")
                    exit(1)
    
    # Initialize recording manager
    try:
        with app.app_context():
            recording_manager = init_recording_manager(app, db)
            logger.info("Recording manager initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize recording manager: {e}")
        exit(1)
    
    # Initialize stream monitor
    try:
        with app.app_context():
            stream_monitor = init_stream_monitor(app, db, recording_manager)
            logger.info("Stream monitor initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize stream monitor: {e}")
        exit(1)
    
    # Debug breadcrumbs: Log environment and tool versions
    try:
        import shutil, subprocess
        if shutil.which("streamlink"):
            v = subprocess.check_output(["streamlink", "--version"], text=True).strip()
            logger.info(f"Detected {v}")
        else:
            logger.warning("streamlink command not found in PATH")
    except Exception as e:
        logger.warning(f"Could not read streamlink --version: {e}")

    logger.info(f"Resolved DOWNLOAD_PATH={get_download_path()}")
    logger.info(f"Resolved LOG_DIR={log_dir}")
    logger.info(f"Python version: {sys.version}")
    
    # Validate container path configuration
    _validate_container_paths()
    
    # Reconcile DB 'recording' rows that have no process
    try:
        with app.app_context():
            stale = Recording.query.filter_by(status='recording').all()
            for r in stale:
                ts_path, part_path = _recording_paths(r)
                ts_exists, part_exists = os.path.exists(ts_path), os.path.exists(part_path)
                if ts_exists and not part_exists:
                    r.status = 'completed'
                elif part_exists and not ts_exists:
                    r.status = 'failed'
                else:
                    r.status = 'failed'
                r.ended_at = r.ended_at or datetime.utcnow()
                if r.started_at and r.ended_at:
                    r.duration = max(0, int((r.ended_at - r.started_at).total_seconds()))
            db.session.commit()
    except Exception as e:
        logger.error(f"Error reconciling stale recordings: {e}")

    # Start conversion worker once
    if not conversion_worker_thread or not conversion_worker_thread.is_alive():
        conversion_worker_thread = threading.Thread(target=conversion_worker_loop, name="conv-worker", daemon=True)
        conversion_worker_thread.start()
        logger.info("Conversion worker thread started")
    
    # Start template scheduler once
    if 'template_scheduler_thread' not in globals() or not globals().get('template_scheduler_thread').is_alive():
        template_scheduler_thread = threading.Thread(target=template_scheduler_loop, name="template-scheduler", daemon=True)
        template_scheduler_thread.start()
        logger.info("Template scheduler thread started")
    
    # Start monitoring for existing active streamers
    try:
        with app.app_context():
            stream_monitor.start_monitoring_all_active()
            logger.info("Started monitoring all active streamers")
    except Exception as e:
        logger.error(f"Error starting stream monitoring on startup: {e}")
    
    # Get port from environment or use non-standard port
    port = int(os.getenv('PORT', 8080))
    
    # Log startup diagnostics
    log_startup_diagnostics()
    
    # Add cleanup on shutdown
    def cleanup():
        try:
            recording_manager.shutdown()
            stream_monitor.shutdown()
            logger.info("Cleanup completed on shutdown")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    atexit.register(cleanup)
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, debug=False)
