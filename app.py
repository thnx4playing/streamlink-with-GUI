#!/usr/bin/env python3
"""
Streamlink Web GUI - A web interface for managing Twitch stream recordings
"""
import os, sys, time, json, threading, subprocess, signal, uuid, logging
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# Import existing modules
from twitch_manager import TwitchManager, StreamStatus
from streamlink_manager import StreamlinkManager
from notification_manager import NotificationManager

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
    "connect_args": {
        "check_same_thread": False,   # required since we use threads
        "timeout": 30,                # sqlite busy timeout in seconds
    }
})

# Initialize extensions
db = SQLAlchemy(app)
CORS(app)

# Configure SQLite for better concurrency (WAL mode and busy timeout)
from sqlalchemy import text
with app.app_context():
    try:
        db.session.execute(text("PRAGMA journal_mode=WAL;"))
        db.session.execute(text("PRAGMA busy_timeout=30000;"))  # 30s
        db.session.commit()
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
        "args": ["-c:v","copy","-c:a","copy"],
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

class Recording(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    streamer_id = db.Column(db.Integer, db.ForeignKey('streamer.id'), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    title = db.Column(db.String(255))
    status = db.Column(db.String(20), default='recording')  # recording, completed, failed, deleted
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    ended_at = db.Column(db.DateTime)
    file_size = db.Column(db.Integer)  # in bytes
    duration = db.Column(db.Integer)  # in seconds
    game = db.Column(db.String(255))  # Game/category being played
    pid = db.Column(db.Integer, nullable=True)
    session_guid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()))

class ConversionSettings(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    volume_path = db.Column(db.String(500))
    output_volume_path = db.Column(db.String(500))  # New: separate output directory
    naming_scheme = db.Column(db.String(50), default='streamer_date_title')
    custom_filename_template = db.Column(db.String(500))  # New: custom naming template
    delete_original_after_conversion = db.Column(db.Boolean, default=False)  # New: delete original option
    ffmpeg_preset = db.Column(db.String(64), nullable=False, default=DEFAULT_FFMPEG_PRESET_KEY)  # New: ffmpeg preset key
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ConversionJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    recording_id = db.Column(db.Integer, db.ForeignKey('recording.id'), nullable=True)  # Allow NULL for template jobs
    status = db.Column(db.String(20), default='pending')  # pending, scheduled, converting, completed, failed
    progress = db.Column(db.String(255))
    output_filename = db.Column(db.String(500))
    scheduled_at = db.Column(db.DateTime)  # New: scheduled time
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
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

# Global variables for managing recording processes
recording_processes = {}
recording_threads = {}
# Track per-recording stop flags for cooperative cancellation and per-recording PIDs
recording_stop_flags = {}  # {recording_id: threading.Event()}
recording_procinfo = {}   # {recording_id: {"pid": int, "proc": Popen, "stop_flag": threading.Event}}
active_by_streamer = {}   # {streamer_id: recording_id}
conversion_worker_thread = None
conversion_wakeup = threading.Event()

def get_download_path():
    """Get the download path from environment or use default"""
    return os.getenv('DOWNLOAD_PATH', './download')

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
    return datetime.utcnow()

def _build_ffmpeg_cmd(input_path, output_path, settings: ConversionSettings):
    """Build FFmpeg command using the selected preset"""
    key = getattr(settings, "ffmpeg_preset", DEFAULT_FFMPEG_PRESET_KEY)
    preset = FFMPEG_PRESETS.get(key, FFMPEG_PRESETS[DEFAULT_FFMPEG_PRESET_KEY])

    # Always include -y and -threads 0; then append preset args
    cmd = ["ffmpeg", "-hide_banner", "-y", "-i", input_path, "-threads", "0"]
    cmd += preset["args"]
    cmd += [output_path]
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

def _eligible_conversion_jobs(batch_size=3):
    """Return next chunk of conversion jobs that should run now."""
    now = _utcnow()
    q = (
        ConversionJob.query
        .filter(ConversionJob.status == 'pending')
        .filter(
            db.or_(
                # Manual/immediate jobs: run regardless of scheduled_at
                ConversionJob.schedule_type.in_(['manual', 'immediate']),
                # Scheduled jobs: run when due
                db.and_(
                    ConversionJob.schedule_type.notin_(['manual', 'immediate']),
                    ConversionJob.scheduled_at.isnot(None),
                    ConversionJob.scheduled_at <= now,
                ),
            )
        )
    )
    # Prefer earlier scheduled_at; if NULL (immediate jobs may be stamped 'now' or be NULL),
    # fall back to primary key order so jobs are processed FIFO-ish.
    try:
        # Some SQLite builds don't support NULLS FIRST syntax via SQLAlchemy;
        # simple two-key order is plenty here:
        q = q.order_by(ConversionJob.scheduled_at.asc(), ConversionJob.id.asc())
    except Exception:
        q = q.order_by(ConversionJob.id.asc())
    return q.limit(batch_size).all()

def _update_job_progress(job, text):
    job.progress = text
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()

def _start_job(job: "ConversionJob"):
    """Mark job as started and commit quickly"""
    job.status = 'converting'
    job.started_at = _utcnow()
    _update_job_progress(job, "Starting conversion...")
    db.session.commit()

def _finalize_job(job: "ConversionJob"):
    """Mark job as completed/failed and commit quickly"""
    job.completed_at = _utcnow()
    db.session.commit()

def _run_ffmpeg_conversion(job: "ConversionJob"):
    """Run the actual FFmpeg conversion (long-running operation)"""
    # Resolve paths & validations (kept defensive)
    rec = Recording.query.get(job.recording_id) if job.recording_id else None
    if not rec:
        job.status = 'failed'
        _update_job_progress(job, 'Recording not found')
        return
    # Only finalized .ts (no .part)
    ts_path = os.path.join(get_download_path(), f"{rec.filename}.ts")
    part_path = os.path.join(get_download_path(), f"{rec.filename}.ts.part")
    if (not os.path.exists(ts_path)) or os.path.exists(part_path) or rec.status in ('deleted','failed','recording'):
        job.status = 'failed'
        _update_job_progress(job, 'Source not eligible')
        return
    out_dir = get_converted_path()
    base_name = job.custom_filename.strip() if job.custom_filename else rec.filename
    mp4_path = os.path.join(out_dir, f"{base_name}.mp4")
    
    # Get conversion settings for preset
    settings = ConversionSettings.query.first()
    if not settings:
        settings = ConversionSettings()
        db.session.add(settings)
        db.session.commit()
    
    # Build ffmpeg cmd using preset
    cmd = _build_ffmpeg_cmd(ts_path, mp4_path, settings)
    _update_job_progress(job, "Starting ffmpeg…")
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        # read a few lines to keep UI alive
        last_line = ''
        while True:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break;
            if line:
                last_line = line.strip()
                if 'time=' in last_line or 'frame=' in last_line:
                    _update_job_progress(job, last_line[-120:])
        rc = proc.wait()
        if rc == 0 and os.path.exists(mp4_path):
            job.status = 'completed'
            _update_job_progress(job, f"Done: {os.path.basename(mp4_path)}")
            if job.delete_original and os.path.exists(ts_path):
                try: os.remove(ts_path)
                except Exception: pass
        else:
            job.status = 'failed'
            _update_job_progress(job, f"ffmpeg failed (rc={rc})")
    except Exception as e:
        job.status = 'failed'
        _update_job_progress(job, f"Exception: {e}")

def conversion_worker_loop():
    """Run in a daemon thread with an active Flask app context."""
    ctx = app.app_context()
    ctx.push()
    logger.info("Conversion worker: started (app context bound)")
    try:
        while True:
            try:
                # Use retry mechanism for database operations
                jobs = _retry_locked(_eligible_conversion_jobs)
                if not jobs:
                    # Sleep lightly but allow immediate wake-up when new jobs are queued
                    conversion_wakeup.wait(timeout=5)
                    conversion_wakeup.clear()
                    continue
                for job in jobs:
                    # Expunge job from session to avoid holding DB connection during FFmpeg
                    db.session.expunge(job)
                    # Mark job as started and commit quickly
                    _retry_locked(_start_job, job)
                    # Run the actual conversion (long-running)
                    _run_ffmpeg_conversion(job)
                    # Mark job as completed/failed and commit quickly
                    _retry_locked(_finalize_job, job)
            except Exception as e:
                logger.exception("Conversion worker loop error: %s", e)
                time.sleep(2)
            finally:
                # Ensure each iteration has a clean session (thread-safe)
                db.session.remove()
    finally:
        try:
            ctx.pop()
        except Exception:
            pass

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
    """Check if a recording can be converted (completed .ts file exists)"""
    if not recording.filename:
        return False
    
    download_path = get_download_path()
    ts_path = os.path.join(download_path, f"{recording.filename}.ts")
    return os.path.exists(ts_path)

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
    cmd = ["streamlink", "--loglevel", "info"]

    # Auth (per Streamlink Twitch plugin docs)
    if auth:
        tok = _normalize_twitch_token(auth.oauth_token or "")
        if tok:
            cmd += [f"--twitch-api-header=Authorization=OAuth {tok}"]
            cmd += ["--http-cookie", f"auth-token={tok}"]
        if auth.client_id:
            cmd += ["--http-header", f"Client-ID={auth.client_id}"]

    # Version-safe stability flags for 7.1.3+
    cmd += [
        "--retry-open", "3",
        "--retry-streams", "3",
        "--stream-timeout", "60",         # replaces deprecated --hls-timeout
        "--stream-segment-timeout", "20", # replaces deprecated --hls-segment-timeout
        "--hls-live-edge", "3",
        "--hls-live-restart",
        "--ringbuffer-size", "16M",
    ]

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

    # URL, quality, output
    cmd += [f"https://twitch.tv/{username}", "best", "-o", f"{ts_out_path}.ts"]
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
    """Start a recording for a specific streamer with duplicate protection"""
    # Block duplicates per streamer
    existing_id = active_by_streamer.get(streamer.id)
    if existing_id:
        t = recording_threads.get(existing_id)
        if t and t.is_alive():
            logger.warning(f"Refusing to start duplicate recording for streamer {streamer.id}; active recording {existing_id} running.")
            return None
        else:
            active_by_streamer.pop(streamer.id, None)

    recording = Recording(streamer_id=streamer.id, filename=make_filename(streamer),
                          title=streamer.title, game=streamer.game, status='recording')
    db.session.add(recording); db.session.commit()

    def _run():
        try:
            download_path = get_download_path()
            base = os.path.join(download_path, recording.filename)
            ts_path = f"{base}.ts"
            part_path = f"{base}.ts.part"
            os.makedirs(download_path, exist_ok=True)

            # Prefer Twitch CLI path for Twitch, regardless of platform label
            auth = TwitchAuth.query.first()
            use_cli = looks_like_twitch(streamer)
            cmd = build_twitch_cli_cmd(streamer.twitch_name, base, auth) if use_cli else build_streamlink_cmd(streamer, ts_path)

            # Per-recording logfile
            rec_log = open(f"{log_dir}/recording_{recording.id}.log", "a", encoding="utf-8")
            def _mask(s: str) -> str:
                if not s: return s
                if "Authorization=OAuth " in s:
                    head, token = s.split("Authorization=OAuth ", 1)
                    token = token.strip()
                    if " " in token:
                        token = token.split(" ")[0]
                    if len(token) > 8:
                        token = token[:4] + "•••" + token[-4:]
                    s = head + "Authorization=OAuth " + token
                if "Client-ID=" in s:
                    head, cid = s.split("Client-ID=", 1)
                    cid = cid.strip()
                    if " " in cid:
                        cid = cid.split(" ")[0]
                    if len(cid) > 8:
                        cid = cid[:4] + "•••" + cid[-4:]
                    s = head + "Client-ID=" + cid
                return s
            rec_log.write(f"[BEGIN] {datetime.utcnow().isoformat()} Recording {recording.id} streamer={streamer.username} use_cli={use_cli} cmd={' '.join(_mask(x) for x in cmd)}\n")

            stop_flag = threading.Event()
            recording_procinfo[recording.id] = {"pid": None, "proc": None, "stop_flag": stop_flag}
            active_by_streamer[streamer.id] = recording.id

            # Launch
            proc_env = os.environ.copy()
            if auth:
                proc_env["STREAMLINK_TWITCH_CLIENT_ID"] = (auth.client_id or "")
                proc_env["STREAMLINK_TWITCH_AUTH_TOKEN"] = _normalize_twitch_token(auth.oauth_token or "")
            proc = subprocess.Popen(cmd, stdout=rec_log, stderr=subprocess.STDOUT, text=True, env=proc_env)
            recording.pid = proc.pid
            db.session.commit()
            recording_procinfo[recording.id]["pid"] = proc.pid
            recording_procinfo[recording.id]["proc"] = proc

            # Wait for process to finish
            rc = proc.wait()
            recording.ended_at = datetime.utcnow()

            # Decide status from process exit first, then disk
            if rc == 0 and os.path.exists(ts_path) and not os.path.exists(part_path):
                recording.status = 'completed'
                try: recording.file_size = os.path.getsize(ts_path)
                except Exception: pass
            else:
                recording.status = 'failed'
                try:
                    size = os.path.getsize(part_path if os.path.exists(part_path) else ts_path)
                    recording.file_size = size
                except Exception:
                    pass

            if recording.started_at and recording.ended_at:
                recording.duration = max(0, int((recording.ended_at - recording.started_at).total_seconds()))

            db.session.commit()
            rec_log.write(f"[END] rc={rc} status={recording.status} ended_at={recording.ended_at.isoformat()}\n")
            rec_log.close()
        except Exception as e:
            logger.exception(f"Recording thread error: {e}")
        finally:
            recording_threads.pop(recording.id, None)
            recording_procinfo.pop(recording.id, None)
            active_by_streamer.pop(streamer.id, None)

    th = threading.Thread(target=_run, name=f"rec-{recording.id}", daemon=True)
    th.start()
    recording_threads[recording.id] = th
    return recording.id

def record_stream(streamer_id):
    """Background function to record a stream"""
    with app.app_context():
        streamer = Streamer.query.get(streamer_id)
        if not streamer:
            return
        
        config = AppConfig(streamer)
        twitch_manager = TwitchManager(config)
        streamlink_manager = StreamlinkManager(config)
        notifier_manager = NotificationManager(config)
        
        download_path = ensure_download_directory()
        
        while streamer.is_active:
            try:
                # Refresh streamer from database to check if still active
                streamer = Streamer.query.get(streamer_id)
                if not streamer or not streamer.is_active:
                    break
                
                stream_status, title = twitch_manager.check_user(config.user)
                
                if stream_status == StreamStatus.ONLINE:
                    # Get additional stream info including game
                    try:
                        user_info = twitch_manager.get_from_twitch('get_users', logins=config.user)
                        if user_info:
                            stream_info = twitch_manager.get_from_twitch('get_streams', user_id=user_info.id)
                            game_name = ""
                            if stream_info and stream_info.game_id:
                                game_info = twitch_manager.get_from_twitch('get_games', game_ids=[stream_info.game_id])
                                if game_info:
                                    game_name = game_info.name
                    except Exception as e:
                        logger.error(f"Error getting game info for {config.user}: {e}")
                        game_name = ""
                    
                    # Create safe filename
                    safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    timestamp = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
                    filename = f"{config.user} - {timestamp} - {safe_title}"
                    recorded_filename = os.path.join(download_path, filename)
                    
                    # Create recording record
                    recording = Recording(
                        streamer_id=streamer_id,
                        filename=filename,
                        title=title,
                        game=game_name,
                        status='recording'
                    )
                    db.session.add(recording)
                    db.session.commit()
                    
                    # Send notification
                    message = f"Recording {config.user} - {title}"
                    notifier_manager.notify_all(message)
                    logger.info(message)
                    
                    # Start recording
                    try:
                        # per-recording logger file
                        os.makedirs(log_dir, exist_ok=True)
                        rec_log_path = f"{log_dir}/recording_{recording.id}.log"
                        _fmt = logging.Formatter("%(asctime)s [%(levelname)s] rec-%(name)s: %(message)s")
                        fh = logging.FileHandler(rec_log_path, encoding="utf-8")
                        fh.setFormatter(_fmt)
                        rec_logger = logging.getLogger(f"rec.{recording.id}")
                        rec_logger.setLevel(logging.INFO)
                        rec_logger.addHandler(fh)
                        # also pipe streamlink's own logs
                        sl_logger = logging.getLogger("streamlink")
                        sl_logger.setLevel(logging.INFO)
                        sl_logger.addHandler(fh)
                        rec_logger.info(f"Begin recording streamer={config.user} id={recording.id} file={recorded_filename}")

                        # cooperative stop flag
                        stop_event = threading.Event()
                        recording_stop_flags[recording.id] = stop_event

                        use_cli = looks_like_twitch(streamer)
                        auth = TwitchAuth.query.first()
                        if use_cli:
                            # Build CLI and launch subprocess; capture stdout to per-recording file
                            cmd = build_twitch_cli_cmd(streamer.twitch_name, recorded_filename, auth)
                            rec_logger.info("CLI path → %s", " ".join(cmd))
                            proc_env = os.environ.copy()
                            if auth:
                                proc_env["STREAMLINK_TWITCH_CLIENT_ID"] = (auth.client_id or "")
                                proc_env["STREAMLINK_TWITCH_AUTH_TOKEN"] = _normalize_twitch_token(auth.oauth_token or "")
                            info = {"pid": None, "proc": None, "stop_flag": stop_event}
                            recording_procinfo[recording.id] = info
                            active_by_streamer[streamer.id] = recording.id
                            proc = subprocess.Popen(
                                cmd,
                                stdout=open(f"{log_dir}/recording_{recording.id}.log", "a", encoding="utf-8"),
                                stderr=subprocess.STDOUT,
                                text=True,
                                env=proc_env
                            )
                            info["pid"] = proc.pid
                            info["proc"] = proc
                            recording.pid = proc.pid
                            db.session.commit()
                            rc = proc.wait()
                            rec_logger.info("CLI exited rc=%s", rc)
                            # mimic library-result shape for status logic below
                            result = {"stopped_by_user": False}
                        else:
                            # Fallback: Python library path
                            result = streamlink_manager.run_streamlink(
                                config.user, recorded_filename, stop_event=stop_event, logger=rec_logger
                            )

                        # finalize status based on stop vs natural end and disk state
                        recording.ended_at = datetime.utcnow()
                        ts_path = f"{recorded_filename}.ts"
                        part_path = f"{recorded_filename}.part"
                        if result.get("stopped_by_user"):
                            recording.status = 'failed'
                            if os.path.exists(part_path):
                                recording.file_size = os.path.getsize(part_path)
                        else:
                            if os.path.exists(ts_path) and not os.path.exists(part_path):
                                recording.status = 'completed'
                                recording.file_size = os.path.getsize(ts_path)
                            elif os.path.exists(part_path) and not os.path.exists(ts_path):
                                recording.status = 'failed'
                                recording.file_size = os.path.getsize(part_path)
                            else:
                                recording.status = 'failed'
                        if recording.started_at:
                            recording.duration = int((recording.ended_at - recording.started_at).total_seconds())
                        db.session.commit()
                        rec_logger.info(f"Finalize status={recording.status} size={recording.file_size} ts_exists={os.path.exists(ts_path)} part_exists={os.path.exists(part_path)}")
                        # teardown handlers
                        try:
                            sl_logger.removeHandler(fh)
                            rec_logger.removeHandler(fh)
                            fh.close()
                        except Exception:
                            pass
                        
                        message = f"Stream {config.user} completed. File saved as {filename}"
                        logger.info(message)
                        notifier_manager.notify_all(message)
                        
                    except Exception as e:
                        recording.status = 'failed'
                        recording.ended_at = datetime.utcnow()
                        db.session.commit()
                        logger.error(f"Recording failed for {config.user}: {e}")
                        
                elif stream_status == StreamStatus.ERROR:
                    logger.error(f"Error checking status for {config.user}")
                    
            except Exception as e:
                logger.error(f"Unexpected error in recording loop for {config.user}: {e}")
            
            time.sleep(config.timer)

# Routes
@app.route('/')
def index():
    """Main dashboard"""
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
    
    # Start recording thread if active
    if streamer.is_active:
        thread = threading.Thread(target=record_stream, args=(streamer.id,), daemon=True)
        thread.start()
        recording_threads[streamer.id] = thread
    
    return jsonify({'message': 'Streamer added successfully', 'id': streamer.id}), 201

@app.route('/api/streamers/<int:streamer_id>', methods=['PUT'])
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
    
    streamer.updated_at = datetime.utcnow()
    db.session.commit()
    
    # Handle recording thread
    if streamer.is_active and streamer_id not in recording_threads:
        thread = threading.Thread(target=record_stream, args=(streamer_id,), daemon=True)
        thread.start()
        recording_threads[streamer_id] = thread
    elif not streamer.is_active and streamer_id in recording_threads:
        # Note: We can't easily stop the thread, but it will stop on next iteration
        del recording_threads[streamer_id]
    
    return jsonify({'message': 'Streamer updated successfully'})

@app.route('/api/streamers/<int:streamer_id>', methods=['DELETE'])
def delete_streamer(streamer_id):
    """API endpoint to delete a streamer"""
    streamer = Streamer.query.get_or_404(streamer_id)
    
    # Stop recording thread
    if streamer_id in recording_threads:
        del recording_threads[streamer_id]
    
    db.session.delete(streamer)
    db.session.commit()
    
    return jsonify({'message': 'Streamer deleted successfully'})

@app.route('/api/recordings', methods=['GET'])
def get_recordings():
    """API endpoint to get recordings"""
    logger.info("GET /api/recordings called")
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        conversion_only = request.args.get('conversion_only', 'false').lower() == 'true'
        
        logger.info(f"Fetching recordings: page={page}, per_page={per_page}, conversion_only={conversion_only}")
        
        # Filter out recordings that have been converted
        # Get all recording IDs that have completed conversion jobs
        converted_recording_ids = db.session.query(ConversionJob.recording_id).filter(
            ConversionJob.status == 'completed'
        ).distinct().all()
        converted_ids = [r[0] for r in converted_recording_ids if r[0] is not None]
        
        # Query recordings excluding converted ones and deleted ones
        recordings_query = Recording.query.filter(Recording.status != 'deleted')
        if converted_ids:
            recordings_query = recordings_query.filter(~Recording.id.in_(converted_ids))
        
        recordings = recordings_query.order_by(Recording.started_at.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        logger.info(f"Found {recordings.total} total recordings")
        
        recordings_data = []
        for r in recordings.items:
            # Only adjust if not deleted and no live process is tracked
            if r.status != 'deleted' and not recording_procinfo.get(r.id):
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
            if conversion_only and not is_recording_convertible(r):
                continue
            
            streamer = Streamer.query.get(r.streamer_id)
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
                'duration': r.duration,
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
def get_active_recordings():
    """API endpoint to get currently active recordings"""
    active_recordings = Recording.query.filter_by(status='recording').all()
    
    recordings_data = []
    for recording in active_recordings:
        streamer = Streamer.query.get(recording.streamer_id)
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
            'duration': recording.duration
        })
    
    return jsonify({'recordings': recordings_data})

@app.route('/api/recordings/<int:recording_id>/stop', methods=['POST'])
def stop_recording(recording_id):
    """API endpoint to stop a recording"""
    recording = Recording.query.get_or_404(recording_id)
    
    if recording.status != 'recording':
        return jsonify({'error': 'Recording is not currently active'}), 400
    
    try:
        # Signal cooperative stop
        flag = recording_stop_flags.get(recording_id)
        if flag:
            flag.set()
        else:
            logger.warning(f"No stop flag for recording {recording_id}; it may have already ended")
        # Give the loop time to flush and finalize (not rename .part)
        wait_until = time.time() + 15
        while time.time() < wait_until:
            ts_path = os.path.join(get_download_path(), f"{recording.filename}.ts")
            part_path = os.path.join(get_download_path(), f"{recording.filename}.part")
            if os.path.exists(part_path) and not os.path.exists(ts_path):
                break
            time.sleep(0.5)
        # Finalize DB status
        recording.ended_at = datetime.utcnow()
        if recording.started_at:
            recording.duration = int((recording.ended_at - recording.started_at).total_seconds())
        ts_path = os.path.join(get_download_path(), f"{recording.filename}.ts")
        part_path = os.path.join(get_download_path(), f"{recording.filename}.part")
        if os.path.exists(ts_path) and not os.path.exists(part_path):
            recording.status = 'completed'
            recording.file_size = os.path.getsize(ts_path)
        elif os.path.exists(part_path) and not os.path.exists(ts_path):
            recording.status = 'failed'
            recording.file_size = os.path.getsize(part_path)
        else:
            recording.status = 'failed'
        db.session.commit()
        return jsonify({'message': 'Recording stop signaled'})
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
        except ValueError:
            return jsonify({'error': 'Invalid scheduled time format'}), 400
    # For immediate jobs, make them due right away so the worker can pick them up
    if schedule_type == 'immediate' and not scheduled_datetime:
        scheduled_datetime = datetime.utcnow()
    
    # Debug logging
    logger.info(f"Conversion request - schedule_type: {schedule_type}, scheduled_time: {scheduled_time}, scheduled_datetime: {scheduled_datetime}")
    
    for recording_id in recording_ids:
        # Check if conversion job already exists
        existing_job = ConversionJob.query.filter_by(recording_id=recording_id).first()
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
        per_page = request.args.get('per_page', 10, type=int)  # Default 10 per page
        
        logger.info(f"Fetching conversion progress: page={page}, per_page={per_page}")
        
        jobs = ConversionJob.query.order_by(ConversionJob.id.desc()).paginate(
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
            logger.info(f"Deleted converted file: {output_file}")
            return jsonify({'message': 'Converted file deleted successfully'})
        else:
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
    streamer_name = streamer.username if streamer else 'unknown'
    twitch_name = streamer.twitch_name if streamer else 'unknown'
    
    # Clean title for filename
    safe_title = "".join(c for c in (recording.title or 'untitled') if c.isalnum() or c in (' ', '-', '_')).rstrip()
    
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
    
    # Ensure it ends with .mp4
    if not filename.endswith('.mp4'):
        filename += '.mp4'
    
    return filename

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
                        "ALTER TABLE recording ADD COLUMN session_guid VARCHAR(36)"
                    ]:
                        try: conn.execute(db.text(ddl))
                        except Exception: pass
                    try: conn.execute(db.text("ALTER TABLE twitch_auth ADD COLUMN extra_flags VARCHAR(512)"))
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
                        new_settings_columns = ['output_volume_path', 'custom_filename_template', 'delete_original_after_conversion']
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

    # Start recording threads for existing active streamers
    try:
        with app.app_context():
            active_streamers = Streamer.query.filter_by(is_active=True).all()
            for streamer in active_streamers:
                # we'll only start on demand via UI / scheduler to avoid duplicates
                logger.info(f"Streamer {streamer.id} is active; waiting for explicit start to avoid duplicates.")
    except Exception as e:
        logger.error(f"Error starting recording threads: {e}")
    
    # Start conversion worker once
    if not conversion_worker_thread or not conversion_worker_thread.is_alive():
        conversion_worker_thread = threading.Thread(target=conversion_worker_loop, name="conv-worker", daemon=True)
        conversion_worker_thread.start()
        logger.info("Conversion worker thread started")
    
    # Get port from environment or use non-standard port
    port = int(os.getenv('PORT', 8080))
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, debug=False)
