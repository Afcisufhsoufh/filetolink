import sys
import logging
import asyncio
import os
import urllib.parse
from quart import Quart, Blueprint, Response, request, render_template_string
from telethon import TelegramClient
from telethon.tl.custom import Message
from datetime import datetime
from mimetypes import guess_type
from math import ceil, floor
import uvicorn

# Initialize logging immediately
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stderr)])
logger = logging.getLogger(__name__)
logger.info("Starting api.py, Python version: %s", sys.version)

class Telegram:
    API_ID = 28239710
    API_HASH = "7fc5b35692454973318b86481ab5eca3"
    BOT_TOKEN = "8072349020:AAFpPnJJFw5F6KMCCbDoaFHCPlZpU3L9Ki0"
    CHANNEL_ID = -1002735511721

class Server:
    BIND_ADDRESS = "0.0.0.0"
    PORT = int(os.environ.get("PORT", 8000))

class HTTPError(Exception):
    def __init__(self, status_code, description=None):
        self.status_code = status_code
        self.description = description
        super().__init__(self.status_code, self.description)

error_messages = {
    400: "Invalid request.",
    401: "File code is required to download the file.",
    403: "Invalid file code.",
    404: "File not found.",
    416: "Invalid range.",
    500: "Internal server error.",
}

def abort(status_code: int = 500, description: str = None):
    raise HTTPError(status_code, description)

def get_file_properties(message: Message):
    file_name = message.file.name
    file_size = message.file.size or 0
    mime_type = message.file.mime_type
    if not file_name:
        attributes = {
            "video": "mp4",
            "audio": "mp3",
            "voice": "ogg",
            "photo": "jpg",
            "video_note": "mp4",
        }
        for attribute in attributes:
            media = getattr(message, attribute, None)
            if media:
                file_type, file_format = attribute, attributes[attribute]
                break
        else:
            abort(400, "Invalid media type.")
        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{file_type}-{date}.{file_format}"
    if not mime_type:
        mime_type = guess_type(file_name)[0] or "application/octet-stream"
    return file_name, file_size, mime_type

class FileLinkAPI(TelegramClient):
    def __init__(self, session_name, api_id, api_hash, bot_token, base_url):
        super().__init__(session_name, api_id, api_hash, connection_retries=2, timeout=15, flood_sleep_threshold=0)
        self.bot_token = bot_token
        self.base_url = base_url.rstrip('/')
        self.in_use = False

    async def start_api(self):
        try:
            logger.info("Attempting to start Telegram client")
            await self.start(bot_token=self.bot_token)
            logger.info("FileLinkAPI started successfully")
        except Exception as e:
            logger.error("Failed to start FileLinkAPI: %s", e)
            raise

app = Quart(__name__)
app.config["RESPONSE_TIMEOUT"] = None
bp = Blueprint("main", __name__)

async def get_streaming_api():
    api = api_instance
    if api.in_use:
        raise HTTPError(503, "No available API instance.")
    api.in_use = True
    return api

async def return_streaming_api(api):
    api.in_use = False

@bp.route("/")
async def home():
    return "File Link API"

@bp.route("/dl/<int:file_id>")
async def transmit_file(file_id):
    selected_api = await get_streaming_api()
    me = await selected_api.get_me()
    api_name = "@" + me.username
    logger.info("File download request - File ID: %s, Using API: %s", file_id, api_name)
    file = None
    try:
        file = await selected_api.get_messages(Telegram.CHANNEL_ID, ids=int(file_id))
        if not file:
            logger.warning("Message %s not found in channel %s", file_id, Telegram.CHANNEL_ID)
            await return_streaming_api(selected_api)
            abort(404)
    except Exception as e:
        logger.error("Failed to retrieve message %s using API %s: %s", file_id, api_name, e)
        await return_streaming_api(selected_api)
        abort(500)
    code = request.args.get("code") or abort(401)
    if code != file.raw_text:
        logger.warning("Access denied - Invalid code for file %s: provided=%s, expected=%s", file_id, code, file.raw_text)
        await return_streaming_api(selected_api)
        abort(403)
    file_name, file_size, mime_type = get_file_properties(file)
    logger.info("File properties - Name: %s, Size: %s, Type: %s", file_name, file_size, mime_type)
    range_header = request.headers.get("Range", 0)
    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
        logger.info("Range request - Bytes: %s-%s/%s", from_bytes, until_bytes, file_size)
    else:
        from_bytes = 0
        until_bytes = file_size - 1
        logger.info("Full file request - Size: %s bytes", file_size)
    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        logger.error("Invalid range request - Bytes: %s-%s/%s", from_bytes, until_bytes, file_size)
        await return_streaming_api(selected_api)
        abort(416, "Invalid range.")
    chunk_size = 2 * 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)
    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1
    req_length = until_bytes - from_bytes + 1
    part_count = ceil(until_bytes / chunk_size) - floor(offset / chunk_size)
    headers = {
        "Content-Type": f"{mime_type}",
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Accept-Ranges": "bytes",
    }
    logger.info("Starting file stream - API: %s, Chunks: %s, Chunk size: %s", api_name, part_count, chunk_size)
    async def file_generator():
        current_part = 1
        prefetch_buffer = asyncio.Queue(maxsize=50)
        async def prefetch_chunks():
            async for chunk in selected_api.iter_download(
                file,
                offset=offset,
                chunk_size=chunk_size,
                stride=chunk_size,
                file_size=file_size,
            ):
                start_time = asyncio.get_event_loop().time()
                await prefetch_buffer.put((chunk, start_time))
        prefetch_task = asyncio.create_task(prefetch_chunks())
        try:
            while current_part <= part_count:
                try:
                    chunk, start_time = await asyncio.wait_for(prefetch_buffer.get(), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.warning("Prefetch timeout - slow network?")
                    continue
                if not chunk:
                    break
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > 0.5:
                    logger.warning("Slow chunk fetch: %.2f seconds", elapsed)
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk
                current_part += 1
            logger.info("File stream completed successfully - File: %s, API: %s", file_name, api_name)
        except Exception as e:
            logger.error("Error during file streaming - File: %s, API: %s, Error: %s", file_name, api_name, e)
            raise
        finally:
            prefetch_task.cancel()
            await return_streaming_api(selected_api)
            logger.info("API %s returned to queue", api_name)
    return Response(file_generator(), headers=headers, status=206 if range_header else 200)

@bp.route("/stream/<int:file_id>")
async def stream_file(file_id):
    code = request.args.get("code") or abort(401)
    quoted_code = urllib.parse.quote(code)
    base_url = api_instance.base_url
    return await render_template_string(
        '<!DOCTYPE html><html lang="en"><head><title>Play Files</title><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><meta http-equiv="X-Frame-Options" content="deny"><link rel="stylesheet" href="https://cdn.plyr.io/3.7.8/plyr.css" /><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css"><script src="https://cdn.plyr.io/3.7.8/plyr.polyfilled.js"></script><style>html, body { margin: 0; height: 100%; }#stream-media { height: 100%; width: 100%; }#error-message { color: red; font-size: 24px; text-align: center; margin-top: 20px; }.plyr__video-wrapper .plyr-download-button, .plyr__video-wrapper .plyr-share-button { position: absolute; top: 10px; left: 10px; width: 30px; height: 30px; background-color: rgba(0, 0, 0, 0.7); border-radius: 50%; text-align: center; line-height: 30px; color: white; z-index: 10; }.plyr__video-wrapper .plyr-share-button { top: 50px; }.plyr__video-wrapper .plyr-download-button:hover, .plyr__video-wrapper .plyr-share-button:hover { background-color: rgba(255, 255, 255, 0.7); color: black; }.plyr__video-wrapper .plyr-download-button:before { font-family: "Font Awesome 5 Free"; content: "\\f019"; font-weight: bold; }.plyr__video-wrapper .plyr-share-button:before { font-family: "Font Awesome 5 Free"; content: "\\f064"; font-weight: bold; }.plyr, .plyr__video-wrapper, .plyr__video-embed iframe { height: 100%; }</style></head><body><video id="stream-media" controls preload="auto"><source src="{{ mediaLink }}" type=""><p class="vjs-no-js">To view this video please enable JavaScript, and consider upgrading to a web browser that supports HTML5 video</p></video><div id="error-message"></div><script>var player = new Plyr("#stream-media", {controls: ["play-large", "rewind", "play", "fast-forward", "progress", "current-time", "mute", "settings", "pip", "fullscreen"],settings: ["speed", "loop"],speed: { selected: 1, options: [0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2] },seek: 10,keyboard: { focused: true, global: true },});var mediaLink = "{{ mediaLink }}";if (mediaLink) {document.querySelector("#stream-media source").setAttribute("src", mediaLink);player.restart();var downloadButton = document.createElement("div");downloadButton.className = "plyr-download-button";downloadButton.onclick = function() {event.stopPropagation();var link = document.createElement("a");link.href = mediaLink;document.body.appendChild(link);link.click();document.body.removeChild(link);};player.elements.container.querySelector(".plyr__video-wrapper").appendChild(downloadButton);var shareButton = document.createElement("div");shareButton.className = "plyr-share-button";shareButton.onclick = function() {event.stopPropagation();if (navigator.share) {navigator.share({ title: "Play", url: mediaLink });}};player.elements.container.querySelector(".plyr__video-wrapper").appendChild(shareButton);} else {document.getElementById("error-message").textContent = "Error: Media URL not provided";}</script></body></html>',
        mediaLink=f"{base_url}/dl/{file_id}?code={quoted_code}"
    )

async def invalid_request(_):
    return "Invalid request.", 400

async def not_found(_):
    return "Resource not found.", 404

async def invalid_method(_):
    return "Invalid request method.", 405

async def http_error(error: HTTPError):
    error_message = error_messages.get(error.status_code)
    return error.description or error_message, error.status_code

app.register_blueprint(bp)
app.register_error_handler(400, invalid_request)
app.register_error_handler(404, not_found)
app.register_error_handler(405, invalid_method)
app.register_error_handler(HTTPError, http_error)

base_url = os.environ.get("HOST_URL", "https://fdlapi-ed9a85898ea5.herokuapp.com" if os.environ.get("DYNO") else "http://localhost:8000")
api_instance = FileLinkAPI(
    session_name="file_link_api",
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.BOT_TOKEN,
    base_url=base_url
)

@app.before_serving
async def startup():
    logger.info("Running startup tasks")
    try:
        await api_instance.start_api()
        logger.info("Startup completed successfully")
    except Exception as e:
        logger.error("Startup failed: %s", e)
        raise

if __name__ == "__main__":
    logger.info("Starting application on %s:%s", Server.BIND_ADDRESS, Server.PORT)
    try:
        uvicorn.run(app, host=Server.BIND_ADDRESS, port=Server.PORT, log_config=None, timeout_keep_alive=120)
    except Exception as e:
        logger.error("Failed to start uvicorn server: %s", e)
        raise
