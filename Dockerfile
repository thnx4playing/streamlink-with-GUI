# Base image with Python (glibc / Debian Trixie)
# FROM python:3.13.7-trixie
FROM container.home.alnet.org/3rd-party/library/python:3.13.7-trixie
WORKDIR /app

# Prevent interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies including FFmpeg
RUN apt-get update && apt-get install -y \
    gcc \
    libc6-dev \
    ffmpeg \
    ca-certificates \
    chromium \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app.py .
COPY streamlink-recorder.py .
COPY twitch_manager.py .
COPY streamlink_manager.py .
COPY notification_manager.py .
COPY recording_manager.py .
COPY stream_monitor.py .
COPY templates/ ./templates/
COPY xvfb_wrapper.sh .
COPY chromium_wrapper .

# Copy favicon (use favicon-new.png as fallback if favicon.png doesn't exist)
COPY favicon*.png ./
RUN if [ -f favicon.png ]; then \
        echo "Using favicon.png"; \
    else \
        echo "favicon.png not found, using favicon-new.png"; \
        mv favicon-new.png favicon.png 2>/dev/null || echo "No favicon files found"; \
    fi

# Create required directories
RUN mkdir -p /app/download /app/data /app/config

# Create non-root user
RUN groupadd -g 1000 streamlink && \
    useradd -m -u 1000 -g streamlink -s /bin/bash streamlink

# Set proper permissions for the streamlink user
RUN chown -R streamlink:streamlink /app && \
    chmod -R 755 /app

# Keep running as root for now to ensure database creation works
# USER streamlink

# Expose the web port
EXPOSE 8080

# Set environment variables
ENV PORT=8080
ENV DOWNLOAD_PATH=/app/download
ENV DBUS_SESSION_BUS_ADDRESS=/dev/null

# Run the web application
#CMD ["python", "app.py"]
ENTRYPOINT ["./xvfb_wrapper.sh"]
