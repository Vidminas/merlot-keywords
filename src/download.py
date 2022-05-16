from collections import namedtuple
import os
from urllib.parse import urlparse, unquote
from pathlib import Path
import asyncio
import aiofiles
import aiofiles.os
import aiohttp
from aiohttp_retry import RetryClient
import pandas as pd

import constants
from utils import gather_unlimited_concurrency


async def download_material(
    session: RetryClient, row: namedtuple,
) -> tuple[bool, str]:
    if not row.URL_OK:
        return row.URL_OK, row.URL_Error_Message
    
    if row.Detected_File_Type not in ("PDF", "Document (e.g. Word)"):
        return row.URL_OK, row.URL_Error_Message

    url_parsed = urlparse(row.Material_URL)
    if row.Detected_File_Type == "PDF":
        url_file_extension = ".pdf"
    else:
        url_file_extension = "".join(Path(unquote(url_parsed.path)).suffixes)

    filename = str(row.Material_ID) + url_file_extension
    filepath = os.path.join(constants.MATERIAL_DOWNLOAD_DIR, filename)

    file_exists = await aiofiles.os.path.exists(filepath)
    if file_exists:
        return True, ""

    try:
        async with session.get(
            row.Material_URL, headers={"User-Agent": constants.USER_AGENT}
        ) as res:
            data = await res.read()
    except (
        aiohttp.ClientConnectorError,
        aiohttp.ClientPayloadError,
        aiohttp.ClientResponseError,
        aiohttp.ClientOSError,
        aiohttp.ServerDisconnectedError,
        asyncio.TimeoutError,
        ConnectionResetError,
    ) as e:
        return False, repr(e)

    async with aiofiles.open(
        file=filepath,
        mode="wb",
    ) as f:
        await f.write(data)

    return True, ""


async def download_materials(material_info: pd.DataFrame):
    # The keep-alive header is to avoid session close after ClientPayloadError
    # https://github.com/aio-libs/aiohttp/issues/3904
    # it occurs with https://www.art.com/, because it uses HTTP/2
    async with RetryClient(
        timeout=constants.NETWORK_DOWNLOAD_TIMEOUT,
        retry_options=constants.NETWORK_RETRY_OPTIONS,
        headers={"Connection": "keep-alive"},
    ) as session:
        results = await gather_unlimited_concurrency(
            "Downloading materials",
            *(download_material(session, row) for row in material_info.itertuples(index=True, name="Material")),
        )

    tuples_as_df = pd.DataFrame(
        results, columns=["URL_OK", "URL_Error_Message"]
    )

    material_info.update(tuples_as_df)
    material_info.to_csv(constants.MATERIALS_DATA_PATH)
    return material_info
