# Streamlink Web GUI Setup Guide

This guide will help you set up and run the Streamlink Web GUI on your system.

## Prerequisites

- Docker and Docker Compose installed
- Twitch API credentials (Client ID, Client Secret, OAuth Token)

## Step 1: Get Twitch API Credentials

1. Go to [Twitch Developer Console](https://dev.twitch.tv/console)
2. Log in with your Twitch account
3. Click "Register Your Application"
4. Fill in the form:
   - **Name**: Streamlink Web GUI (or any name you prefer)
   - **OAuth Redirect URLs**: `http://localhost:8080`
   - **Category**: Application Integration
5. Click "Create"
6. Note down your **Client ID** and **Client Secret**

### Getting OAuth Token

1. Go to [Twitch Token Generator](https://twitchtokengenerator.com/)
2. Select "Custom Scope Token"
3. Add these scopes:
   - `user:read:email`
   - `user:read:follows`
   - `user:read:subscriptions`
4. Click "Generate Token"
5. Note down your **Access Token**

## Step 2: Configure Environment

1. Copy the environment template:
   ```bash
   cp env.example .env
   ```

2. Edit the `.env` file with your Twitch credentials and download path:
   ```env
   TWITCH_CLIENT_ID=your_client_id_here
   TWITCH_CLIENT_SECRET=your_client_secret_here
   TWITCH_OAUTH_TOKEN=your_oauth_token_here
   DOWNLOAD_VOLUME_PATH=/path/to/your/download/directory
   ```

## Step 3: Start the Web GUI

### Running on a Remote Docker Server

If you have a separate Docker server, you can run the application there and access it remotely:

1. **Copy the project to your Docker server**
2. **SSH into your Docker server**
3. **Follow the setup steps on the server**
4. **Access the web GUI** from your local machine at `http://YOUR_SERVER_IP:2344`

### Option A: Using the startup script (Recommended)

**On Linux/Mac:**
```bash
./start.sh
```

**On Windows:**
```cmd
start.bat
```

### Option B: Manual Docker Compose

```bash
# For Docker Compose V2 (modern installations)
docker compose -f docker-compose.web.yml up -d

# For Docker Compose V1 (older installations)
docker-compose -f docker-compose.web.yml up -d
```

## Step 4: Access the Web Interface

Open your web browser and go to:
```
http://localhost:2344
```

## Step 5: Add Your First Streamer

1. Click the "Add Streamer" button
2. Fill in the form:
   - **Username**: A friendly name for the streamer
   - **Twitch Name**: The actual Twitch username (e.g., "caseoh_")
   - **Quality**: Choose your preferred quality (best, 720p, etc.)
   - **Timer**: Check interval in seconds (360 recommended)
   - **Active**: Check to start monitoring immediately
3. Click "Add Streamer"

## Features

### Dashboard
- View active streamers count
- See total recordings
- Check today's recordings
- Monitor download path

### Streamer Management
- Add new streamers
- Edit existing streamers
- Enable/disable streamers
- Delete streamers
- Real-time status updates

### Recording History
- View all recordings
- See recording status (recording, completed, failed)
- Check file sizes and durations
- Paginated results

### Configuration
- Set recording quality
- Configure check intervals
- Manage notifications
- View system status

## Troubleshooting

### Common Issues

1. **"Twitch API Error"**
   - Check your Twitch credentials in `.env`
   - Ensure OAuth token is valid and not expired

2. **"Port 2344 already in use"**
   - Change the PORT in `.env` file
   - Or stop the service using port 2344

3. **"Database error"**
   - Check if the `data` directory exists and is writable
   - Restart the container: `docker-compose -f docker-compose.web.yml restart`

4. **"Recording not starting"**
   - Check if the streamer is actually live on Twitch
   - Verify the Twitch username is correct
   - Check container logs: `docker-compose -f docker-compose.web.yml logs -f`

### Viewing Logs

```bash
# View all logs (Docker Compose V2)
docker compose -f docker-compose.web.yml logs -f

# View all logs (Docker Compose V1)
docker-compose -f docker-compose.web.yml logs -f

# View specific service logs
docker compose -f docker-compose.web.yml logs -f streamlink-web
```

### Stopping the Service

```bash
# Docker Compose V2
docker compose -f docker-compose.web.yml down

# Docker Compose V1
docker-compose -f docker-compose.web.yml down
```

## API Usage

The web GUI provides a RESTful API for programmatic access:

```bash
# Get all streamers
curl http://localhost:2344/api/streamers

# Add a new streamer
curl -X POST http://localhost:2344/api/streamers \
  -H "Content-Type: application/json" \
  -d '{"username":"test","twitch_name":"testuser","quality":"best","timer":360,"is_active":true}'

# Get system status
curl http://localhost:2344/api/status
```

## File Locations

- **Downloads**: `./download/` (mounted to `/app/download` in container)
- **Database**: `./data/` (mounted to `/app/data` in container)
- **Logs**: Docker container logs

## Security Notes

- Change the `SECRET_KEY` in production
- Use HTTPS in production environments
- Regularly rotate your Twitch OAuth tokens
- Keep your `.env` file secure and never commit it to version control

## Support

If you encounter issues:

1. Check the troubleshooting section above
2. View the container logs
3. Verify your Twitch API credentials
4. Check the [original project issues](https://github.com/liofal/streamlink/issues) for similar problems
