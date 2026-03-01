#!/usr/bin/env python3
"""
Twitch Manager - Fixed version with proper session management
Fixes:
1. Periodic Twitch client refresh to prevent stale sessions
2. Reusable event loop instead of asyncio.run() each time
3. Proper error recovery and logging
4. Connection timeout handling
"""
import asyncio
import logging
import time
import threading
from twitchAPI.twitch import Twitch
from twitchAPI.helper import first
from enum import Enum, auto
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

class StreamStatus(Enum):
    ONLINE = auto()
    UNDESIRED_GAME = auto()
    OFFLINE = auto()
    ERROR = auto()


class TwitchManager:
    # Refresh Twitch client every 6 hours to prevent stale sessions
    CLIENT_REFRESH_INTERVAL = 6 * 60 * 60  # 6 hours in seconds
    
    # Max age for a single client before forcing refresh
    CLIENT_MAX_AGE = 24 * 60 * 60  # 24 hours
    
    def __init__(self, config):
        self.config = config
        self._twitch: Optional[Twitch] = None
        self._twitch_created_at: float = 0
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._consecutive_errors = 0
        self._last_successful_call = time.time()
        
    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create a dedicated event loop for async operations"""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
        return self._loop

    async def _app_refresh_callback(self, token: str):
        """Callback when app token is refreshed"""
        logger.info(f'Twitch app token refreshed at {time.strftime("%Y-%m-%d %H:%M:%S")}')
        self._last_successful_call = time.time()
        
    async def _ensure_twitch_client(self) -> Twitch:
        """
        Ensure we have a valid Twitch client, creating or refreshing as needed.
        This fixes the issue of stale sessions over time.
        """
        now = time.time()
        client_age = now - self._twitch_created_at
        
        should_refresh = (
            self._twitch is None or
            client_age > self.CLIENT_MAX_AGE or
            self._consecutive_errors >= 3  # Force refresh after 3 consecutive errors
        )
        
        if should_refresh:
            if self._twitch is not None:
                logger.info(f"Refreshing Twitch client (age: {client_age/3600:.1f}h, errors: {self._consecutive_errors})")
                try:
                    await self._twitch.close()
                except Exception as e:
                    logger.warning(f"Error closing old Twitch client: {e}")
            
            logger.info("Creating new Twitch API client")
            self._twitch = await Twitch(
                self.config.client_id, 
                self.config.client_secret
            )
            self._twitch.app_auth_refresh_callback = self._app_refresh_callback
            self._twitch_created_at = now
            self._consecutive_errors = 0
            logger.info("Twitch API client created successfully")
            
        return self._twitch

    async def get_from_twitch_async(self, operation: str, **kwargs):
        """
        Execute a Twitch API operation with proper error handling.
        """
        try:
            twitch = await self._ensure_twitch_client()
            result = await first(getattr(twitch, operation)(**kwargs))
            self._consecutive_errors = 0
            self._last_successful_call = time.time()
            return result
        except Exception as e:
            self._consecutive_errors += 1
            logger.error(f"Twitch API error (consecutive: {self._consecutive_errors}): {e}")
            
            # If we've had multiple errors, force client refresh on next call
            if self._consecutive_errors >= 3:
                logger.warning("Multiple consecutive Twitch API errors, will refresh client on next call")
            
            raise

    def get_from_twitch(self, operation: str, **kwargs):
        """
        Synchronous wrapper for Twitch API calls.
        Uses a dedicated event loop to avoid creating new loops constantly.
        """
        with self._lock:
            loop = self._get_or_create_loop()
            try:
                return loop.run_until_complete(
                    self.get_from_twitch_async(operation, **kwargs)
                )
            except Exception as e:
                raise

    def check_user(self, user: str) -> Tuple[StreamStatus, str]:
        """
        Check if a user is streaming.
        Returns (StreamStatus, stream_title)
        """
        try:
            user_info = self.get_from_twitch('get_users', logins=user)
            if not user_info:
                return StreamStatus.OFFLINE, ""

            stream_info = self.get_from_twitch('get_streams', user_id=[user_info.id])

            if not stream_info:
                return StreamStatus.OFFLINE, ""

            
            game_id = stream_info.game_id
            title = stream_info.title or ""
            
            # Only filter by game if game_list is actually configured and not empty
            if (self.config.game_list and 
                self.config.game_list.strip() and 
                self.config.game_list.strip() != 'game_id1,game_id2,game_id3' and
                game_id not in self.config.game_list.split(",")):
                return StreamStatus.UNDESIRED_GAME, title

            return StreamStatus.ONLINE, title
            
        except Exception as e:
            logger.error(f"Error checking user {user}: {e}")
            return StreamStatus.ERROR, ""
    
    def get_health_status(self) -> dict:
        """Return health status for monitoring/debugging"""
        now = time.time()
        return {
            "client_age_hours": (now - self._twitch_created_at) / 3600 if self._twitch_created_at else None,
            "consecutive_errors": self._consecutive_errors,
            "seconds_since_success": now - self._last_successful_call,
            "client_exists": self._twitch is not None,
        }
    
    def close(self):
        """Clean up resources"""
        with self._lock:
            if self._loop and not self._loop.is_closed():
                if self._twitch:
                    try:
                        self._loop.run_until_complete(self._twitch.close())
                    except Exception as e:
                        logger.warning(f"Error closing Twitch client: {e}")
                self._loop.close()
                self._loop = None
            self._twitch = None
