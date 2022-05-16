import json
import os
import requests
from aiohttp_retry import RetryClient

from constants import LICENSE_KEY_VAR


def load_cached_response(request_type: str):
    filepath = os.path.join("cache", request_type)
    if os.path.exists(filepath) and os.path.isfile(filepath):
        with open(file=filepath, mode="r", encoding="utf-8") as f:
            return json.load(f)
    return None


def cache_response(request_type: str, response: requests.Response):
    if not os.path.exists("cache"):
        os.mkdir("cache")

    filepath = os.path.join("cache", request_type)
    with open(file=filepath, mode="w", encoding="utf-8") as f:
        json.dump(response, f, ensure_ascii=False)


def merlot_cached_request(request_url: str, redownload: bool):
    # get string after last slash in URL
    request_type = request_url.rsplit("/", 1)[-1]

    if not redownload:
        cached_response = load_cached_response(request_type)
        if cached_response is not None:
            return cached_response

    response = requests.get(
        url=request_url, params={"licenseKey": os.getenv(LICENSE_KEY_VAR)}
    )
    json = response.json()
    cache_response(request_type, json)
    return json


def merlot_material_types_request(redownload=False):
    return merlot_cached_request(
        "https://www.merlot.org/merlot/materialTypes.json", redownload
    )


def merlot_technical_formats_request(redownload=False):
    return merlot_cached_request(
        "https://www.merlot.org/merlot/technicalFormats.json", redownload
    )


def merlot_material_audiences_request(redownload=False):
    return merlot_cached_request(
        "https://www.merlot.org/merlot/materialAudiences.json", redownload
    )


def merlot_languages_request(redownload=False):
    return merlot_cached_request(
        "https://www.merlot.org/merlot/languages.json", redownload
    )


# Always redownload
def merlot_search_request(params={}):
    params["licenseKey"] = os.getenv(LICENSE_KEY_VAR)
    response = requests.get(
        url="https://www.merlot.org/merlot/materialsAdvanced.json", params=params
    )
    return response.json()


async def merlot_async_search_page(session: RetryClient, page_num: int):
    async with session.get(
        url="https://www.merlot.org/merlot/materialsAdvanced.json",
        params={
            "licenseKey": os.getenv(LICENSE_KEY_VAR),
            "page": page_num,
        },
    ) as response:
        return await response.json()
