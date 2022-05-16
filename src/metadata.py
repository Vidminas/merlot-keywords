import asyncio
from tqdm import tqdm
import aiofiles
import aiohttp
from aiohttp_retry import RetryClient
import itertools
import mimetypes
from urllib.parse import urlparse
import magic
import json
import pandas as pd
from typing import Optional

import constants
from datatypes import FILETYPES, MERLOTMaterial, UrlReturnType
from utils import gather_unlimited_concurrency
from merlot_api import (
    merlot_async_search_page,
    merlot_languages_request,
    merlot_technical_formats_request,
)


async def merlot_async_search_all_pages():
    async with RetryClient(
        timeout=constants.NETWORK_DOWNLOAD_TIMEOUT,
        retry_options=constants.NETWORK_RETRY_OPTIONS,
    ) as session:
        first_page_results = await merlot_async_search_page(session, 1)
        num_materials = first_page_results["nummaterialstotal"]
        num_pages = num_materials // len(first_page_results["results"])

        other_page_results = await gather_unlimited_concurrency(
            "Downloading MERLOT metadata",
            *(
                merlot_async_search_page(session, page_num)
                for page_num in range(2, 2 + num_pages)
            ),
        )

        results_per_page = [
            page_results["results"]
            for page_results in (first_page_results, *other_page_results)
        ]
        flattened_results = list(itertools.chain(*results_per_page))
        return flattened_results


async def download_merlot_metadata():
    formats = merlot_technical_formats_request()["formats"]
    doc_format = [format for format in formats if format.startswith("Document")]

    languages = merlot_languages_request()["languages"]
    en_language = [lang["code"] for lang in languages if lang["name"] == "English"]

    all_results = await merlot_async_search_all_pages()
    data = json.dumps(all_results, ensure_ascii=False)

    async with aiofiles.open(
        file=constants.MERLOT_METADATA_PATH, mode="w", encoding="utf-8"
    ) as f:
        await f.write(data)


def load_merlot_metadata() -> list[MERLOTMaterial]:
    with open(file=constants.MERLOT_METADATA_PATH, mode="r", encoding="utf-8") as f:
        materials_json: list[dict] = json.load(f)

    materials = []

    for m in tqdm(materials_json, desc="Parsing metadata"):
        material: MERLOTMaterial = MERLOTMaterial.from_dict(m, infer_missing=True)
        materials.append(material)

    return materials


async def test_material_url(
    session: RetryClient, material: MERLOTMaterial
) -> UrlReturnType:
    # Skip files hosted on MERLOT (haven't yet found a way to download them)
    parsed_url = urlparse(material.url)
    if not parsed_url.netloc or not parsed_url.scheme:
        return (material, False, "Skipped relative URL")

    if parsed_url.scheme != "http" and parsed_url.scheme != "https":
        return (material, False, f"Unsupported url scheme '{parsed_url.scheme}'")

    # If not guessed, try downloading and parsing the first few bytes to tell the filetype
    try:
        async with session.head(
            material.url,
            allow_redirects=True,
            headers={"User-Agent": constants.USER_AGENT},
        ) as response:
            if not response.ok:
                return (material, False, f"Got response status {response.status}")

            if response.content_length is None:
                return (material, False, "Got response with empty content")

            if response.content_length < constants.BYTES_TO_PEEK:
                return (
                    material,
                    False,
                    f"Got response with less than {constants.BYTES_TO_PEEK} bytes of content",
                )
    except (
        aiohttp.ClientConnectorError,
        aiohttp.ClientResponseError,
        aiohttp.ClientOSError,
        aiohttp.ServerDisconnectedError,
        asyncio.TimeoutError,
        ConnectionResetError,
    ) as e:
        return (material, False, repr(e))
    except ValueError as e:
        # 1 is a bug in aiohttp, happens when a website redirects to a URL that doesn't work
        # occurs with http://libraries.ucsd.edu/speccoll/dswenttowar
        # which redirects to https, and works fine in a browser, but the exception says the redirect url isn't absolute
        # https://github.com/aio-libs/aiohttp/pull/6722
        #
        # 2 is another bug / missing feature: https://github.com/aio-libs/aiohttp/issues/2507
        # occurs with http://www.fusiontechnology.in/data-science-course.php
        if (
            e.args[0] == "URL should be absolute"
            or e.args[0] == "Can redirect only to http or https"
        ):
            return (material, False, repr(e))
        else:
            print(f"{e}: {material.url}")
            raise e
    except Exception as e:
        print(f"{e}: {material.url}")
        raise e
    else:
        return (material, True, "OK")


async def test_all_urls(materials: list[MERLOTMaterial]) -> list[UrlReturnType]:
    # The keep-alive header is to avoid ClientPayloadError
    # https://github.com/aio-libs/aiohttp/issues/3904
    # it occurs with https://www.art.com/, because it uses HTTP/2
    async with RetryClient(
        timeout=constants.NETWORK_CHECK_TIMEOUT,
        retry_options=constants.NETWORK_RETRY_OPTIONS,
        headers={"Connection": "keep-alive"},
    ) as session:
        return await gather_unlimited_concurrency(
            "Testing material URLs",
            *(test_material_url(session, material) for material in materials),
        )


def known_websites_filetypes(url: str) -> Optional[FILETYPES]:
    parsed_url = urlparse(url)
    if parsed_url.hostname == "www.youtube.com":
        return "Video"

    return None


def map_mime_to_filetype(mimetype: str) -> FILETYPES:
    if mimetype == "text/html":
        return "Website"
    if mimetype == "application/pdf":
        return "PDF"
    if mimetype == "application/octet-stream":
        return "Executable Program"
    if mimetype.startswith("image"):
        return "Image"
    if mimetype.startswith("audio"):
        return "Audio File (e.g. Podcast)"
    if mimetype.startswith("video"):
        return "Video"
    if mimetype in (
        "application/zip",
        "application/x-compressed-zip",
        "application/x-compress",
        "application/x-compressed",
        "application/x-zip-compressed",
    ):
        return "Zip"
    if mimetype in (
        "application/powerpoint",
        "application/mspowerpoint",
        "application/x-mspowerpoint",
        "application/vnd.ms-powerpoint",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.oasis.opendocument.presentation",
    ):
        return "Presentation (e.g. PowerPoint)"
    if mimetype in (
        "text/richtext",
        "application/rtf",
        "application/x-rtf",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.oasis.opendocument.text",
    ):
        return "Document (e.g. Word)"
    if mimetype in (
        "application/excel",
        "application/x-excel",
        "application/x-msexcel",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.oasis.opendocument.spreadsheet",
    ):
        return "Spreadsheet (e.g. Excel)"
    if mimetype == "application/x-shockwave-flash":
        return "Flash"
    return "Unsure"


async def guess_filetype(
    session: RetryClient, material: MERLOTMaterial
) -> tuple[MERLOTMaterial, FILETYPES]:
    # Try to guess the MIME type based on the file extension in the URL
    mimetype, _ = mimetypes.guess_type(material.url)
    if mimetype is not None:
        return material, map_mime_to_filetype(mimetype)

    guesstype = known_websites_filetypes(material.url)
    if guesstype is not None:
        return material, guesstype

    try:
        async with session.get(
            material.url, headers={"User-Agent": constants.USER_AGENT}
        ) as response:
            peek = await response.content.read(constants.BYTES_TO_PEEK)
            mimetype = magic.from_buffer(peek, mime=True)
            if mimetype is not None:
                return material, map_mime_to_filetype(mimetype)
    except Exception as e:
        print(f"{e}: {material.url}")

    return material, "Unsure"


async def guess_all_filetypes(
    material_urls: list[UrlReturnType],
) -> list[tuple[MERLOTMaterial, FILETYPES]]:
    async with RetryClient(
        timeout=constants.NETWORK_DOWNLOAD_TIMEOUT,
        retry_options=constants.NETWORK_RETRY_OPTIONS,
    ) as session:
        return await gather_unlimited_concurrency(
            "Checking material filetypes",
            *(
                guess_filetype(session, material)
                for (material, url_ok, _) in material_urls
                if url_ok
            ),
        )


def save_material_info_csv(
    material_urls: list[UrlReturnType],
    material_filetypes: list[tuple[MERLOTMaterial, FILETYPES]],
):
    data = {
        material.materialid: {
            "MERLOT_URL": material.detailURL,
            "Material_URL": material.url,
            "URL_OK": url_ok,
            "URL_Error_Message": url_error_message,
            "Metadata_File_Type": material.technicalFormat,
            "Detected_File_Type": "N/A",
        }
        for material, url_ok, url_error_message in material_urls
    }

    for material, filetype in material_filetypes:
        data[material.materialid]["Detected_File_Type"] = filetype

    df = pd.DataFrame.from_dict(
        data,
        orient="index",
        columns=[
            "MERLOT_URL",
            "Material_URL",
            "URL_OK",
            "URL_Error_Message",
            "Metadata_File_Type",
            "Detected_File_Type",
        ],
    )
    df.index.rename("Material_ID", inplace=True)
    df.to_csv(constants.MATERIALS_DATA_PATH)


def load_material_info_csv() -> pd.DataFrame:
    return pd.read_csv(constants.MATERIALS_DATA_PATH)


def save_broken_urls_csv(material_info: pd.DataFrame):
    def is_broken(row: pd.Series):
        if row["URL_OK"]:
            return False

        error = row["URL_Error_Message"]

        if not isinstance(error, str):
            return False
        if error == "Got response status 404":
            return True
        if error.startswith("Cannot connect to host"):
            if error.find("SSLV3_ALERT_HANDSHAKE_FAILURE") != -1:
                return False
            if error.find("unable to get local issuer certificate") != -1:
                return False
            return True

        return False

    broken_urls = material_info[material_info.apply(is_broken, axis=1)]
    broken_urls.set_index("Material_ID", inplace=True)
    broken_urls.to_csv(constants.BROKEN_URLS_DATA_PATH)


def save_mismatched_filetypes_csv(material_info: pd.DataFrame):
    def is_mismatch(row: pd.Series):
        detected_filetype = row["Detected_File_Type"]
        if not isinstance(detected_filetype, str):
            return False
        if detected_filetype == "N/A":
            return False
        if detected_filetype == "Unsure":
            return False
        # Skipping websites, because most webpages get tagged as such
        # including, e.g., sites that have only a PDF download button
        if detected_filetype == "Website":
            return False
        metadata_filetype = row["Metadata_File_Type"]
        if not isinstance(metadata_filetype, str):
            return True
        if detected_filetype in row["Metadata_File_Type"]:
            return False
        return True

    mismatched_filetypes = material_info[material_info.apply(is_mismatch, axis=1)]
    mismatched_filetypes.set_index("Material_ID", inplace=True)
    mismatched_filetypes.to_csv(constants.MISMATCHED_FILETYPES_DATA_PATH)
