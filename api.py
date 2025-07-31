import asyncio
import re
import secrets
from datetime import datetime as dt
from typing import Any, Dict, AsyncGenerator
from urllib.parse import quote
from cachetools import TTLCache
from pyrogram import Client
from pyrogram.errors import FloodWait, PeerIdInvalid
from pyrogram.types import Message
from pyrogram.file_id import FileId
import aiohttp
from aiohttp import web
from config import LOG_CHANNEL_ID, API_ID, API_HASH, BOT_TOKEN
from pyrogram.enums import ChatMemberStatus

SECURE_HASH_LENGTH = 6
CHUNK_SIZE = 1024 * 1024 * 4  # Increased to 4MB for faster streaming
MAX_CONCURRENT_TASKS = 20  # Increased to handle more concurrent streams
CACHE_TTL = 3600
STREAM_TIMEOUT = 30  # Timeout for streaming in seconds

routes = web.RouteTableDef()
work_loads = {0: 0}
streamers = {}
metadata_cache = TTLCache(maxsize=1000, ttl=CACHE_TTL)
task_queue = asyncio.Queue()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")
PATTERN_HASH_FIRST = re.compile(rf"^(stream|download|api/link)/([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
VALID_HASH_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

class FileNotFound(Exception):
    pass

class InvalidHash(Exception):
    pass

class Logger:
    @staticmethod
    def debug(msg, exc_info=False):
        print(f"DEBUG: {msg}")

    @staticmethod
    def error(msg, exc_info=False):
        print(f"ERROR: {msg}")

    @staticmethod
    def info(msg):
        print(f"INFO: {msg}")

logger = Logger()

app = Client("FileToLinkAPI", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

async def handle_flood_wait(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            logger.debug(f"FloodWait: {func.__name__}, sleep {e.value}s")
            await asyncio.sleep(e.value)

async def resolve_channel(client: Client, chat_id: int) -> bool:
    try:
        logger.debug(f"Resolving peer for channel {chat_id}")
        await client.resolve_peer(chat_id)
        logger.debug(f"Successfully resolved peer for channel {chat_id}")
        return True
    except PeerIdInvalid as e:
        logger.error(f"PeerIdInvalid: Cannot resolve channel {chat_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error resolving channel {chat_id}: {e}", exc_info=True)
        return False

async def check_channel_access(client: Client, chat_id: int) -> bool:
    try:
        if not await resolve_channel(client, chat_id):
            logger.error(f"Failed to resolve channel {chat_id} before checking access")
            return False
        logger.debug(f"Checking API client's admin status in channel {chat_id}")
        member = await client.get_chat_member(chat_id, "me")
        if member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
            logger.debug(f"API client is admin in channel {chat_id}")
            return True
        else:
            logger.error(f"API client is not an admin in channel {chat_id}, status: {member.status}")
            return False
    except PeerIdInvalid as e:
        logger.error(f"PeerIdInvalid: API client cannot access channel {chat_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error checking channel access for {chat_id}: {e}", exc_info=True)
        return False

def get_media(message: Message) -> Any:
    logger.debug(f"Checking media in message {message.id}")
    for attr in dir(message):
        media = getattr(message, attr, None)
        if media and hasattr(media, 'file_id') and hasattr(media, 'file_unique_id'):
            logger.debug(f"Found media type: {attr}")
            return media
    logger.debug("No media found in message")
    return None

def get_uniqid(message: Message) -> str:
    media = get_media(message)
    return getattr(media, 'file_unique_id', None)

def get_hash(media_msg: Message) -> str:
    uniq_id = get_uniqid(media_msg)
    return uniq_id[:SECURE_HASH_LENGTH] if uniq_id else ''

def get_fsize(message: Message) -> int:
    media = get_media(message)
    return getattr(media, 'file_size', 0) if media else 0

def get_fname(msg: Message) -> str:
    media = get_media(msg)
    fname = getattr(media, 'file_name', None) if media else None
    if not fname:
        mime_type = getattr(media, 'mime_type', None) if media else None
        ext = "bin"
        if mime_type:
            ext = mime_type.split('/')[-1] if '/' in mime_type else "bin"
        elif media:
            media_type = type(media).__name__.lower()
            ext = {
                "photo": "jpg",
                "audio": "mp3",
                "voice": "ogg",
                "video": "mp4",
                "animation": "mp4",
                "video_note": "mp4",
                "sticker": "webp"
            }.get(media_type, "bin")
        timestamp = dt.now().strftime("%Y%m%d%H%M%S")
        fname = f"FileToLink_{timestamp}.{ext}"
    logger.debug(f"Generated file name: {fname}")
    return fname

async def get_fids(client: Client, chat_id: int, message_id: int) -> FileId:
    async with semaphore:
        try:
            logger.debug(f"Fetching message {message_id} from chat {chat_id}")
            msg = await handle_flood_wait(client.get_messages, chat_id, message_id)
            if not msg or getattr(msg, 'empty', False):
                raise FileNotFound("Message not found")
            media = get_media(msg)
            if media and hasattr(media, 'file_id') and hasattr(media, 'file_unique_id'):
                return FileId.decode(media.file_id)
            raise FileNotFound("No valid media in message")
        except Exception as e:
            logger.error(f"Error in get_fids: {e}", exc_info=True)
            raise FileNotFound(str(e))

class ByteStreamer:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.chat_id = LOG_CHANNEL_ID

    async def get_message(self, message_id: int) -> Message:
        async with semaphore:
            try:
                if not await check_channel_access(self.client, self.chat_id):
                    raise FileNotFound(f"API client lacks access to channel {self.chat_id}")
                logger.debug(f"Fetching message {message_id} from LOG_CHANNEL")
                message = await handle_flood_wait(self.client.get_messages, self.chat_id, message_id)
                if not message or not message.media:
                    raise FileNotFound(f"Message {message_id} not found")
                return message
            except Exception as e:
                logger.debug(f"Error fetching message {message_id}: {e}", exc_info=True)
                raise FileNotFound(f"Message {message_id} not found") from e

    async def stream_file(self, message_id: int, offset: int = 0, limit: int = 0) -> AsyncGenerator[bytes, None]:
        async with semaphore:
            message = await self.get_message(message_id)
            chunk_offset = offset // CHUNK_SIZE
            chunk_limit = (limit + CHUNK_SIZE - 1) // CHUNK_SIZE if limit > 0 else 0
            logger.debug(f"Streaming file {message_id} with offset {offset} and limit {limit}")
            start_time = dt.now()
            try:
                async for chunk in self.client.stream_media(message, offset=chunk_offset, limit=chunk_limit, timeout=STREAM_TIMEOUT):
                    duration = (dt.now() - start_time).total_seconds()
                    logger.debug(f"Streamed chunk for message {message_id}, duration: {duration:.2f}s")
                    yield chunk
                logger.debug(f"Completed streaming file {message_id}")
            except asyncio.TimeoutError:
                logger.error(f"Streaming timed out for message {message_id} after {STREAM_TIMEOUT}s")
                raise FileNotFound(f"Streaming timeout after {STREAM_TIMEOUT}s")
            except FloodWait as e:
                logger.debug(f"FloodWait: stream_file, sleep {e.value}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(f"Error streaming file {message_id}: {e}", exc_info=True)
                raise FileNotFound(f"Streaming error: {str(e)}")

    async def get_file_info(self, message_id: int) -> Dict[str, Any]:
        cache_key = f"{message_id}"
        if cache_key in metadata_cache:
            logger.debug(f"Cache hit for message {message_id}")
            return metadata_cache[cache_key]
        async with semaphore:
            try:
                message = await self.get_message(message_id)
                media = get_media(message)
                if not media:
                    return {"message_id": message.id, "error": "No media"}
                file_info = {
                    "message_id": message.id,
                    "file_size": getattr(media, 'file_size', 0) or 0,
                    "file_name": get_fname(message),
                    "mime_type": getattr(media, 'mime_type', None) or "application/octet-stream",
                    "unique_id": getattr(media, 'file_unique_id', None),
                    "media_type": type(media).__name__.lower()
                }
                metadata_cache[cache_key] = file_info
                return file_info
            except Exception as e:
                logger.debug(f"Error getting file info for {message_id}: {e}", exc_info=True)
                return {"message_id": message_id, "error": str(e)}

def get_streamer(client_id: int) -> ByteStreamer:
    if client_id not in streamers:
        streamers[client_id] = ByteStreamer(app)
    return streamers[client_id]

def parse_media_request(path: str, query: dict) -> tuple[int, str, str]:
    clean_path = path.strip('/')
    if clean_path.startswith('/'):
        clean_path = clean_path[1:]  # Remove extra leading slashes
    match = PATTERN_HASH_FIRST.match(clean_path)
    if match:
        try:
            action = match.group(1)
            message_id = int(match.group(3))
            secure_hash = match.group(2)
            if len(secure_hash) == SECURE_HASH_LENGTH and VALID_HASH_REGEX.match(secure_hash):
                logger.debug(f"Parsed request: action={action}, secure_hash={secure_hash}, message_id={message_id}")
                return message_id, secure_hash, action
            else:
                logger.error(f"Invalid hash length or format: {secure_hash}")
                raise InvalidHash(f"Hash length must be {SECURE_HASH_LENGTH} or invalid characters")
        except ValueError as e:
            logger.error(f"Invalid message ID format in path {clean_path}: {e}")
            raise InvalidHash(f"Invalid message ID format: {e}") from e
    logger.error(f"Invalid URL structure for path: {clean_path}")
    raise InvalidHash("Invalid URL structure or missing hash")

def select_optimal_client() -> tuple[int, ByteStreamer]:
    if not work_loads:
        raise web.HTTPInternalServerError(text="No available clients.")
    client_id = min(work_loads, key=work_loads.get)
    return client_id, get_streamer(client_id)

def parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    if not range_header:
        return 0, file_size - 1
    match = RANGE_REGEX.match(range_header)
    if not match:
        raise web.HTTPBadRequest(text=f"Invalid range header: {range_header}")
    start = int(match.group("start")) if match.group("start") else 0
    end = int(match.group("end")) if match.group("end") else file_size - 1
    if start < 0 or end >= file_size or start > end:
        raise web.HTTPRequestRangeNotSatisfiable(
            headers={"Content-Range": f"bytes */{file_size}"}
        )
    return start, end

async def queue_worker():
    while True:
        task = await task_queue.get()
        try:
            await task
        except Exception as e:
            logger.error(f"Queue task error: {e}", exc_info=True)
        finally:
            task_queue.task_done()

@routes.get("/")
async def homepage(request: web.Request):
    return web.Response(text="File To Link API Is Alive ✅", content_type="text/plain")

@routes.get(r"/{action:(stream|download)}/{path:.+}", allow_head=True)
async def media_delivery(request: web.Request):
    logger.debug(f"Received web request for path: {request.match_info['path']}, action: {request.match_info['action']}")
    try:
        path = request.match_info["path"]
        action = request.match_info["action"]
        message_id, secure_hash, req_action = parse_media_request(f"{action}/{path}", request.query)
        if action != req_action:
            raise InvalidHash("Action mismatch in URL")
        client_id, streamer = select_optimal_client()
        work_loads[client_id] += 1
        try:
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get('unique_id'):
                raise FileNotFound("File unique ID not found.")
            if file_info['unique_id'][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash("Hash mismatch.")
            file_size = file_info.get('file_size', 0)
            if file_size == 0:
                raise FileNotFound("File size is zero.")
            range_header = request.headers.get("Range", "")
            start, end = parse_range_header(range_header, file_size)
            content_length = end - start + 1
            mime_type = file_info.get('mime_type') or 'application/octet-stream'
            filename = file_info.get('file_name') or f"file_{secrets.token_hex(4)}"
            headers = {
                "Content-Type": mime_type,
                "Content-Length": str(content_length),
                "Content-Disposition": f"{'attachment' if action == 'download' else 'inline'}; filename*=UTF-8''{quote(filename)}",
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000",
                "Connection": "keep-alive"
            }
            if range_header:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
            logger.debug(f"{'Streaming' if action == 'stream' else 'Downloading'} file {message_id} with range {start}-{end}")
            async def stream_generator():
                try:
                    bytes_sent = 0
                    bytes_to_skip = start % CHUNK_SIZE
                    async for chunk in streamer.stream_file(message_id, offset=start, limit=content_length):
                        if bytes_to_skip > 0:
                            if len(chunk) <= bytes_to_skip:
                                bytes_to_skip -= len(chunk)
                                continue
                            chunk = chunk[bytes_to_skip:]
                            bytes_to_skip = 0
                        remaining = content_length - bytes_sent
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]
                        if chunk:
                            yield chunk
                            bytes_sent += len(chunk)
                        if bytes_sent >= content_length:
                            break
                finally:
                    work_loads[client_id] -= 1
            return web.Response(
                status=206 if range_header else 200,
                body=stream_generator(),
                headers=headers
            )
        except (FileNotFound, InvalidHash):
            work_loads[client_id] -= 1
            raise
        except Exception as e:
            work_loads[client_id] -= 1
            error_id = secrets.token_hex(6)
            logger.error(f"Stream error {error_id}: {e}", exc_info=True)
            raise web.HTTPInternalServerError(text=f"Server error: {error_id}") from e
    except (InvalidHash, FileNotFound) as e:
        logger.debug(f"Client error: {type(e).__name__} - {e}", exc_info=True)
        raise web.HTTPNotFound(text=f"Resource not found: {str(e)}") from e
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"Server error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(text=f"Server error: {error_id}") from e

@routes.get(r"/api/link/{path:.+}")
async def api_link(request: web.Request):
    logger.debug(f"Received API request for path: {request.match_info['path']}")
    try:
        path = request.match_info["path"]
        message_id, secure_hash, req_action = parse_media_request(f"api/link/{path}", request.query)
        if req_action != "api/link":
            raise InvalidHash("Action mismatch in URL")
        client_id, streamer = select_optimal_client()
        try:
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get('unique_id'):
                return web.json_response({
                    "status": "error",
                    "message": "File unique ID not found."
                }, status=404)
            if file_info['unique_id'][:SECURE_HASH_LENGTH] != secure_hash:
                return web.json_response({
                    "status": "error",
                    "message": "Hash mismatch."
                }, status=400)
            file_size = file_info.get('file_size', 0)
            if file_size == 0:
                return web.json_response({
                    "status": "error",
                    "message": "File size is zero."
                }, status=404)
            base_url = "https://filetolink-production-f396.up.railway.app"
            return web.json_response({
                "status": "success",
                "message": "File To Link API Is Alive ✅",
                "file": {
                    "message_id": file_info.get("message_id"),
                    "file_name": file_info.get("file_name"),
                    "file_size": file_info.get("file_size"),  # Return raw bytes for bot to handle conversion
                    "mime_type": file_info.get("mime_type"),
                    "media_type": file_info.get("media_type"),
                    "stream_link": f"{base_url}/stream/{secure_hash}{message_id}",
                    "download_link": f"{base_url}/download/{secure_hash}{message_id}"
                }
            })
        except (FileNotFound, InvalidHash) as e:
            return web.json_response({
                "status": "error",
                "message": str(e)
            }, status=404)
        except Exception as e:
            error_id = secrets.token_hex(6)
            logger.error(f"API error {error_id}: {e}", exc_info=True)
            return web.json_response({
                "status": "error",
                "message": f"Server error: {error_id}"
            }, status=500)
    except (InvalidHash, FileNotFound) as e:
        logger.debug(f"Client error: {type(e).__name__} - {e}", exc_info=True)
        return web.json_response({
            "status": "error",
            "message": f"Resource not found: {str(e)}"
        }, status=404)
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"Server error {error_id}: {e}", exc_info=True)
        return web.json_response({
            "status": "error",
            "message": f"Server error: {error_id}"
        }, status=500)

async def main():
    await app.start()
    logger.info("API Pyrogram client started")
    if not await check_channel_access(app, LOG_CHANNEL_ID):
        logger.error("API client failed to start: No access to LOG_CHANNEL")
        await app.stop()
        return
    for _ in range(MAX_CONCURRENT_TASKS):
        asyncio.create_task(queue_worker())
    web_app = web.Application()
    web_app.add_routes(routes)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()
    logger.info("Web server started on port 8000")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
