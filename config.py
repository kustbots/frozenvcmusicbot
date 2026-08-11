import os

API_ID = int(os.getenv("API_ID", "29568441"))
API_HASH = os.getenv("API_HASH", "b32ec0fb66d22da6f77d355fbace4f2a")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SESSION_STRING = os.getenv("ASSISTANT_SESSION") or os.getenv("STRING_SESSION")
MAIN_OWNER = int(os.getenv("OWNER_ID", "8673494392"))
DEPLOYED_OWNER_ID = int(os.getenv("OWNER_ID", "8673494392"))
SEARCH_API_URL = os.getenv("SEARCH_API_URL", "https://search-api.kustbotsweb.workers.dev")
DOWNLOAD_API_BASE = os.getenv("DOWNLOAD_API_BASE", "").rstrip("/")
COOKIES_FILE = os.getenv("COOKIES_FILE", "cookies.txt")
YOUTUBE_COOKIES = os.getenv("YOUTUBE_COOKIES", "")
RATE_LIMIT_COUNT = 4
RATE_LIMIT_WINDOW = 6
MAX_TITLE_LEN = 30
PORT = int(os.getenv("PORT", "8080"))
