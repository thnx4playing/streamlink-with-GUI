#!/usr/bin/env python3
"""
Streamlink Web GUI - A web interface for managing Twitch stream recordings
"""
import os
import json
import threading
import time
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from dotenv import load_dotenv

# Import existing modules
from twitch_manager import TwitchManager, StreamStatus
from streamlink_manager import StreamlinkManager
from notification_manager import NotificationManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# Initialize extensions
db = SQLAlchemy(app)
CORS(app)

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
    status = db.Column(db.String(20), default='recording')  # recording, completed, failed
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    ended_at = db.Column(db.DateTime)
    file_size = db.Column(db.Integer)  # in bytes
    duration = db.Column(db.Integer)  # in seconds
    game = db.Column(db.String(255))  # Game/category being played

class ConversionSettings(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    volume_path = db.Column(db.String(500))
    naming_scheme = db.Column(db.String(50), default='streamer_date_title')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ConversionJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    recording_id = db.Column(db.Integer, db.ForeignKey('recording.id'), nullable=False)
    status = db.Column(db.String(20), default='pending')  # pending, converting, completed, failed
    progress = db.Column(db.String(255))
    output_filename = db.Column(db.String(500))
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

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

def get_download_path():
    """Get the download path from environment or use default"""
    return os.getenv('DOWNLOAD_PATH', './download')

def ensure_download_directory():
    """Ensure the download directory exists"""
    download_path = get_download_path()
    os.makedirs(download_path, exist_ok=True)
    return download_path

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
                        streamlink_manager.run_streamlink(config.user, recorded_filename)
                        
                        # Update recording status
                        recording.status = 'completed'
                        recording.ended_at = datetime.utcnow()
                        
                        # Calculate file size and duration
                        full_path = f"{recorded_filename}.ts"
                        if os.path.exists(full_path):
                            recording.file_size = os.path.getsize(full_path)
                            recording.duration = int((recording.ended_at - recording.started_at).total_seconds())
                        
                        db.session.commit()
                        
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
        
        logger.info(f"Fetching recordings: page={page}, per_page={per_page}")
        
        recordings = Recording.query.order_by(Recording.started_at.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        logger.info(f"Found {recordings.total} total recordings")
        
        recordings_data = []
        for r in recordings.items:
            streamer = Streamer.query.get(r.streamer_id)
            recordings_data.append({
                'id': r.id,
                'streamer_id': r.streamer_id,
                'streamer_name': streamer.username if streamer else 'Unknown',
                'filename': r.filename,
                'title': r.title,
                'game': r.game,
                'status': r.status,
                'started_at': r.started_at.isoformat(),
                'ended_at': r.ended_at.isoformat() if r.ended_at else None,
                'file_size': r.file_size,
                'duration': r.duration
            })
        
        response_data = {
            'recordings': recordings_data,
            'total': recordings.total,
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
            'started_at': recording.started_at.isoformat(),
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
        # Update recording status
        recording.status = 'completed'
        recording.ended_at = datetime.utcnow()
        
        # Calculate duration
        if recording.started_at:
            recording.duration = int((recording.ended_at - recording.started_at).total_seconds())
        
        # Calculate file size
        download_path = get_download_path()
        full_path = f"{os.path.join(download_path, recording.filename)}.ts"
        if os.path.exists(full_path):
            recording.file_size = os.path.getsize(full_path)
        
        db.session.commit()
        
        # Stop the recording thread for this streamer
        if recording.streamer_id in recording_threads:
            # Note: We can't easily stop the thread, but it will stop on next iteration
            # when it checks if the streamer is still active
            pass
        
        return jsonify({'message': 'Recording stopped successfully'})
    except Exception as e:
        logger.error(f"Error stopping recording {recording_id}: {e}")
        return jsonify({'error': 'Failed to stop recording'}), 500

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
            'volume_path': settings.volume_path or '',
            'naming_scheme': settings.naming_scheme
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
    
    settings.volume_path = data.get('volume_path', '')
    settings.naming_scheme = data.get('naming_scheme', 'streamer_date_title')
    settings.updated_at = datetime.utcnow()
    
    db.session.commit()
    
    return jsonify({'message': 'Settings saved successfully'})

@app.route('/api/convert-recordings', methods=['POST'])
def convert_recordings():
    """API endpoint to start conversion of recordings"""
    data = request.get_json()
    recording_ids = data.get('recording_ids', [])
    
    for recording_id in recording_ids:
        # Check if conversion job already exists
        existing_job = ConversionJob.query.filter_by(recording_id=recording_id).first()
        if not existing_job:
            job = ConversionJob(recording_id=recording_id)
            db.session.add(job)
    
    db.session.commit()
    
    # Start conversion process in background
    thread = threading.Thread(target=process_conversions, daemon=True)
    thread.start()
    
    return jsonify({'message': 'Conversion started'})

@app.route('/health')
def health_check():
    """Health check endpoint for Docker"""
    return jsonify({'status': 'healthy'})

@app.route('/api/conversion-progress')
def get_conversion_progress():
    """API endpoint to get conversion progress"""
    jobs = ConversionJob.query.all()
    
    conversions = []
    for job in jobs:
        recording = Recording.query.get(job.recording_id)
        conversions.append({
            'recording_id': job.recording_id,
            'filename': recording.filename if recording else 'Unknown',
            'status': job.status,
            'progress': job.progress,
            'output_filename': job.output_filename
        })
    
    return jsonify({'conversions': conversions})

def process_conversions():
    """Background function to process conversions using FFmpeg"""
    with app.app_context():
        while True:
            pending_jobs = ConversionJob.query.filter_by(status='pending').all()
            
            for job in pending_jobs:
                try:
                    job.status = 'converting'
                    job.progress = 'Starting conversion...'
                    db.session.commit()
                    
                    # Get recording details
                    recording = Recording.query.get(job.recording_id)
                    if not recording:
                        job.status = 'failed'
                        job.progress = 'Recording not found'
                        db.session.commit()
                        continue
                    
                    # Get conversion settings
                    settings = ConversionSettings.query.first()
                    if not settings:
                        settings = ConversionSettings()
                        db.session.add(settings)
                        db.session.commit()
                    
                    # Build output filename based on naming scheme
                    output_filename = build_output_filename(recording, settings.naming_scheme)
                    
                    # Get input and output paths
                    download_path = get_download_path()
                    input_file = os.path.join(download_path, f"{recording.filename}.ts")
                    output_file = os.path.join(download_path, output_filename)
                    
                    # Check if input file exists
                    if not os.path.exists(input_file):
                        job.status = 'failed'
                        job.progress = f'Input file not found: {input_file}'
                        db.session.commit()
                        continue
                    
                    # Check if output file already exists
                    if os.path.exists(output_file):
                        job.status = 'failed'
                        job.progress = f'Output file already exists: {output_filename}'
                        db.session.commit()
                        continue
                    
                    # Update progress
                    job.progress = 'Converting TS to MP4 using FFmpeg...'
                    db.session.commit()
                    
                    # Run FFmpeg conversion
                    success = convert_ts_to_mp4(input_file, output_file, job)
                    
                    if success:
                        job.status = 'completed'
                        job.progress = 'Conversion completed successfully'
                        job.output_filename = output_filename
                        job.completed_at = datetime.utcnow()
                        db.session.commit()
                        logger.info(f"Successfully converted {input_file} to {output_file}")
                    else:
                        job.status = 'failed'
                        job.progress = 'FFmpeg conversion failed'
                        db.session.commit()
                    
                except Exception as e:
                    job.status = 'failed'
                    job.progress = f'Error: {str(e)}'
                    db.session.commit()
                    logger.error(f"Error in conversion job {job.id}: {e}")
            
            time.sleep(10)  # Check for new jobs every 10 seconds

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
                logger.info("Database created successfully")
                
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
    
    # Start recording threads for existing active streamers
    try:
        with app.app_context():
            active_streamers = Streamer.query.filter_by(is_active=True).all()
            for streamer in active_streamers:
                thread = threading.Thread(target=record_stream, args=(streamer.id,), daemon=True)
                thread.start()
                recording_threads[streamer.id] = thread
    except Exception as e:
        logger.error(f"Error starting recording threads: {e}")
    
    # Start conversion processing thread
    try:
        conversion_thread = threading.Thread(target=process_conversions, daemon=True)
        conversion_thread.start()
        logger.info("Conversion processing thread started")
    except Exception as e:
        logger.error(f"Error starting conversion thread: {e}")
    
    # Get port from environment or use non-standard port
    port = int(os.getenv('PORT', 8080))
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, debug=False)
