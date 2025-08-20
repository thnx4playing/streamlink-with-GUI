#!/bin/bash

# Create required directories if they don't exist
mkdir -p /app/download
mkdir -p /app/data
mkdir -p /app/config

# Set proper permissions
chmod 755 /app/download /app/data /app/config

# Start the Flask application
exec python /app/app.py
