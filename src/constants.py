import os
from aiohttp_retry import ExponentialRetry
from aiohttp import (
    ClientTimeout,
    ClientConnectorError,
    ClientResponseError,
    ClientOSError,
    ServerDisconnectedError,
)
from asyncio import TimeoutError

os.makedirs("materials", exist_ok=True)
MERLOT_METADATA_PATH = os.path.join("materials", "metadata.json")
MATERIALS_DATA_PATH = os.path.join("materials", "results.csv")
BROKEN_URLS_DATA_PATH = os.path.join("materials", "broken_urls.csv")
MISMATCHED_FILETYPES_DATA_PATH = os.path.join("materials", "mismatched_filetypes.csv")

os.makedirs(os.path.join("materials", "downloaded"), exist_ok=True)
MATERIAL_DOWNLOAD_DIR = os.path.join("materials", "downloaded")

os.makedirs(os.path.join("materials", "bag_of_words"), exist_ok=True)
MATERIAL_BAG_OF_WORDS_DIR = os.path.join("materials", "bag_of_words")

CORPUS_VOCABULARY_PATH = os.path.join("materials", "corpus_vocab.json")
CORPUS_INVERSE_VOCABULARY_PATH = os.path.join("materials", "corpus_inv_vocab.json")
TF_IDF_DATA_PATH = os.path.join("materials", "tf_idf_results.csv")

LICENSE_KEY_VAR = "MERLOT_LICENSE_KEY"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/101.0.4951.54 Safari/537.36 Edg/101.0.1210.39"
)
NETWORK_RETRY_OPTIONS = ExponentialRetry(
    attempts=3,
    exceptions=[
        ClientConnectorError,
        ClientResponseError,
        ClientOSError,
        ServerDisconnectedError,
        TimeoutError,
    ],
)
# The timeouts are in seconds, applied to each HTTP request
# total timeout includes pool waiting time: https://github.com/aio-libs/aiohttp/issues/3203
NETWORK_CHECK_TIMEOUT = ClientTimeout(total=None, sock_connect=30, sock_read=60)
NETWORK_DOWNLOAD_TIMEOUT = ClientTimeout(total=None, sock_connect=60, sock_read=5 * 60)
BYTES_TO_PEEK = 256
# In theory, the socket select() limit is 64, but in practice, this has to be lower
# Crashes with limit >= 40
# Works with limit <= 35
NETWORK_CONCURRENCY_LIMIT = 35

NUM_STOP_WORDS = 200
NUM_MAX_KEYWORDS = 5
TF_IDF_SCORE_THRESHOLD = 0.02
