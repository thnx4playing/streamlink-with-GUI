#!/bin/bash

# Streamlink Web GUI Startup Script

echo "🚀 Starting Streamlink Web GUI..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Creating from template..."
    cp env.example .env
    echo "📝 Please edit .env file with your Twitch API credentials before starting again."
    echo "   Required variables: TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, TWITCH_OAUTH_TOKEN"
    exit 1
fi

# Check if required environment variables are set
source .env

if [ -z "$TWITCH_CLIENT_ID" ] || [ "$TWITCH_CLIENT_ID" = "your_twitch_client_id_here" ]; then
    echo "❌ TWITCH_CLIENT_ID not configured in .env file"
    exit 1
fi

if [ -z "$TWITCH_CLIENT_SECRET" ] || [ "$TWITCH_CLIENT_SECRET" = "your_twitch_client_secret_here" ]; then
    echo "❌ TWITCH_CLIENT_SECRET not configured in .env file"
    exit 1
fi

if [ -z "$TWITCH_OAUTH_TOKEN" ] || [ "$TWITCH_OAUTH_TOKEN" = "your_twitch_oauth_token_here" ]; then
    echo "❌ TWITCH_OAUTH_TOKEN not configured in .env file"
    exit 1
fi

# Create necessary directories
mkdir -p download
mkdir -p data

echo "✅ Environment configured successfully"
echo "🌐 Starting web GUI on port ${PORT:-2344}..."

# Start the web GUI
docker compose -f docker-compose.web.yml up -d

echo "🎉 Streamlink Web GUI is starting!"
echo "📱 Access the web interface at: http://localhost:${PORT:-2344}"
echo "📊 View logs with: docker compose -f docker-compose.web.yml logs -f"
echo "🛑 Stop with: docker compose -f docker-compose.web.yml down"
