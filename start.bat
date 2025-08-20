@echo off
chcp 65001 >nul

echo 🚀 Starting Streamlink Web GUI...

REM Check if .env file exists
if not exist .env (
    echo ⚠️  .env file not found. Creating from template...
    copy env.example .env
    echo 📝 Please edit .env file with your Twitch API credentials before starting again.
    echo    Required variables: TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, TWITCH_OAUTH_TOKEN
    pause
    exit /b 1
)

REM Create necessary directories
if not exist download mkdir download
if not exist data mkdir data

echo ✅ Environment configured successfully
echo 🌐 Starting web GUI on port 2344...

REM Start the web GUI
docker compose -f docker-compose.web.yml up -d

echo 🎉 Streamlink Web GUI is starting!
echo 📱 Access the web interface at: http://localhost:2344
echo 📊 View logs with: docker compose -f docker-compose.web.yml logs -f
echo 🛑 Stop with: docker compose -f docker-compose.web.yml down

pause
