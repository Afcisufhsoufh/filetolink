from quart import Quart, Blueprint, Response, request, render_template_string
from telethon import TelegramClient
from telethon.tl.custom import Message
from datetime import datetime
from mimetypes import guess_type
from math import ceil, floor
import logging
import asyncio
import urllib.parse

class Telegram:
    API_ID = 28239710
    API_HASH = "7fc5b35692454973318b86481ab5eca3"
    BOT_TOKEN = "8306187507:AAENX5VYgcvN-_DseXvMUYjheWOejC81IwQ"
    CHANNEL_ID = -1002735511721

class Server:
    BASE_URL = "https://filetolink-production-f396.up.railway.app"
    BIND_ADDRESS = "0.0.0.0"
    PORT = 8000

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
        date = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
        file_name = f"{file_type}-{date}.{file_format}"
    if not mime_type:
        mime_type = guess_type(file_name)[0] or "application/octet-stream"
    return file_name, file_size, mime_type

class FileLinkAPI(TelegramClient):
    def __init__(self, session_name, api_id, api_hash, bot_token):
        super().__init__(session_name, api_id, api_hash, connection_retries=-1, timeout=120, flood_sleep_threshold=0)
        self.bot_token = bot_token
        self.base_url = Server.BASE_URL.rstrip('/')
        self.in_use = False

    async def start_api(self):
        await self.start(bot_token=self.bot_token)
        logger.info("FileLinkAPI started")

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
    base_url = Server.BASE_URL.rstrip('/')
    selected_api = await get_streaming_api()
    try:
        file = await selected_api.get_messages(Telegram.CHANNEL_ID, ids=int(file_id))
        if not file:
            logger.warning("Message %s not found in channel %s", file_id, Telegram.CHANNEL_ID)
            await return_streaming_api(selected_api)
            abort(404)
        if code != file.raw_text:
            logger.warning("Access denied - Invalid code for file %s: provided=%s, expected=%s", file_id, code, file.raw_text)
            await return_streaming_api(selected_api)
            abort(403)
        file_name, file_size, mime_type = get_file_properties(file)
        await return_streaming_api(selected_api)
        return await render_template_string(
            """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{file_name}}</title>
    <link rel="icon" href="https://i.ibb.co/Hh4kF2b/icon.png" type="image/x-icon">
    <link rel="shortcut icon" href="https://i.ibb.co/Hh4kF2b/icon.png" type="image/x-icon">
    <link rel="stylesheet" href="https://unpkg.com/sheryjs/dist/Shery.css" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/Jisshubot/data@main/fs/src/style.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/Jisshubot/data@main/fs/src/plyr.css">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Josefin+Sans:wght@500;700&display=swap" rel="stylesheet">
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body>
    <nav>
        <div class="nleft">
            <a href="#">
                <h3 id="heading" style="z-index: 100;" class="magnet title">FILE STREAM BY @ISmartCoder </h3>
            </a>
        </div>
        <div class="nryt">
            <a class="home-btn magnet" href="#main" onclick="toggleWidthnav(this)">HOME</a>
            <a href="#abtus" class="about-btn magnet" onclick="toggleWidthnav(this)">ABOUT</a>
        </div>
    </nav>
    <center>
        <div class="about-nav">
            <a href="#abtus" class="wlcm magnet" onclick="toggleWidth(this)">WELCOME</a>
            <a href="#channels" class="abt-chnl magnet" onclick="toggleWidth(this)">CHANNELS</a>
            <a href="#contact" class="magnet contact-btn" onclick="toggleWidth(this)">CONTACT</a>
        </div>
    </center>
    <div class="outer">
        <div class="inner">
            <div class="main" id="main">
                <video id="player" class="player" src="{{file_url}}" type="video/mp4" playsinline controls></video>
                <div class="player"></div>
                <div class="file-name">
                    <h4 style="display: inline;">File Name: </h4>
                    <p style="display: inline;" id="myDiv">{{file_name}}</p><br>
                    <h4 style="display: inline;">File Size: </h4>
                    <p style="display: inline;">{{file_size}}</p>
                </div>
                <div class="downloadBtn">
                    <button class="magnet" onclick="streamDownload()">
                        <img style="height: 30px;" src="https://i.ibb.co/RjzYttX/dl.png" alt="">download video
                    </button>
                    <button class="magnet" onclick="copyStreamLink()">
                        <img src="https://i.ibb.co/CM4Y586/link.png" alt="Copy Link">copy link
                    </button>
                    <button class="magnet" onclick="vlc_player()">
                        <img src="https://i.ibb.co/px6fQs1/vlc.png" alt="">watch in VLC PLAYER
                    </button>
                    <button class="magnet" onclick="mx_player()">
                        <img src="https://i.ibb.co/41WvtQ3/mx.png" alt="">watch in MX PLAYER
                    </button>
                    <button class="magnet" onclick="n_player()">
                        <img src="https://i.ibb.co/Hd2dS4t/nPlayer.png" alt="">watch in nPlayer
                    </button>
                </div>
            </div>
            <div class="abt">
                <div class="about">
                    <div class="about-dets">
                        <div class="abt-sec" id="abtus" style="padding: 160px 30px;">
                            <h1 style="text-align: center;">WELCOME TO OUR <Span>FILE STREAM</Span> BOT</h1>
                            <p style="text-align: center; line-height: 2;word-spacing: 2px; letter-spacing: 0.8px;">
                                This is a Telegram Bot to Stream <span>Files</span> and <span>Media</span> directly on
                                Telegram. You can also
                                <span>download</span> them if you want. This bot is developed by <a
                                    href="https://telegram.dog/ISmartCoder"><span style="font-weight: 700;">Abir Arafat Chawdhury</span></a>
                                <br><br>If you like this bot, then don't
                                forget to share it with your friends and family.
                            </p>
                        </div>
                        <div class="abt-sec" id="channels">
                            <h1>JOIN OUR <span>TELEGRAM</span> CHANNELS</h1>
                            <div class="links chnl-link">
                                <a class="magnet" href="https://t.me/TheSmartDev">
                                    <button>BOTS CHANNEL</button>
                                </a>
                                <a class="magnet" href="https://t.me/Bot_Bug_Test">
                                    <button>SUPPORT GROUP</button>
                                </a>
                                <a class="magnet" href="https://t.me/ISmartCoder">
                                    <button>Dev's CHANNEL</button>
                                </a>
                            </div>
                        </div>
                        <div class="abt-sec" id="contact">
                            <p style="text-align: center;">Report Bugs and Contact us on Telegram Below</p>
                            <div class="links contact">
                                <a href="https://t.me/ISmartCoder">
                                    <button>CONTACT</button>
                                </a>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <footer>
            <center>
                <div class="copyright">
                    <h5 class="text-center">Copyright © 2024 <a href="https://telegram.dog/ISmartCoder"><span
                                style="font-weight: 700;">TheSmartDev</span></a>. All
                        Rights Reserved.</h5>
                </div>
            </center>
        </footer>
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.2/gsap.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.2/ScrollTrigger.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/0.155.0/three.min.js"></script>
    <script src="https://cdn.jsdelivr.net/gh/automat/controlkit.js@master/bin/controlKit.min.js"></script>
    <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/sheryjs/dist/Shery.js"></script>
    <script>
        document.addEventListener("DOMContentLoaded", function () {
            const uncopyableElement = document.querySelector(".uncopyable");
            if (uncopyableElement) {
                uncopyableElement.addEventListener("selectstart", function (event) {
                    event.preventDefault();
                });
            }
            const player = new Plyr("#player", {
                controls: ["play-large", "rewind", "play", "fast-forward", "progress", "current-time", "mute", "settings", "pip", "fullscreen"],
                settings: ["speed", "loop"],
                speed: { selected: 1, options: [0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2] },
                seek: 10,
                keyboard: { focused: true, global: true }
            });
            function streamDownload() {
                const link = document.createElement("a");
                link.href = "{{file_url}}";
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            }
            function copyStreamLink() {
                navigator.clipboard.writeText("{{file_url}}").then(() => {
                    alert("Link copied to clipboard!");
                });
            }
            function vlc_player() {
                window.location.href = "vlc://{{file_url}}";
            }
            function mx_player() {
                window.location.href = "intent://{{file_url}}#Intent;package=com.mxtech.videoplayer.ad;end";
            }
            function n_player() {
                window.location.href = "nplayer-{{file_url}}";
            }
        });
    </script>
    <script src="https://cdn.plyr.io/3.6.9/plyr.js"></script>
    <script src="https://Jisshubot.github.io/data/fs/src/script.js"></script>
</body>
</html>
""",
            file_url=f"{base_url}/dl/{file_id}?code={quoted_code}",
            file_name=file_name,
            file_size=f"{file_size} bytes"
        )
    except Exception as e:
        await return_streaming_api(selected_api)
        logger.error("Error in stream_file: %s", e)
        raise

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

api_instance = FileLinkAPI(
    session_name="file_link_api",
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.BOT_TOKEN
)

async def main():
    await api_instance.start_api()
    logger.info("API running, starting web server...")
    from uvicorn import Server as UvicornServer, Config
    server = UvicornServer(Config(app=app, host=Server.BIND_ADDRESS, port=Server.PORT, log_config=None, timeout_keep_alive=120))
    logger.info("Web server is started! Running on %s:%s", Server.BIND_ADDRESS, Server.PORT)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
