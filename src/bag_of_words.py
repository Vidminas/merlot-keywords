from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
import re
import json
import os
from typing import Optional
import zipfile
from xml.etree.cElementTree import XML
import asyncio
import concurrent.futures
import aiofiles
import aiofiles.os
import pdfplumber
import pandas as pd
from olefile import OleFileIO

from doc_parser import DOCParser

import constants
from utils import gather_unlimited_concurrency


class CleanupPatterns:
    ban_list = re.compile(
        r"(?:cid:\d+)|(?:\\uf\w{3})|(?:https?://)|(?:www\.)", re.UNICODE
    )
    start_pattern = re.compile(r"^[\W_]+", re.UNICODE)
    end_pattern = re.compile(r"[\W_]+$", re.UNICODE)

    @classmethod
    def cleanup_term(cls, term: str) -> str:
        if cls.ban_list.search(term):
            return ""

        term = cls.start_pattern.sub("", term)
        term = cls.end_pattern.sub("", term)
        return term


def parse_pdf_text(pdf_bytes: bytes) -> tuple[Optional[dict], Optional[Exception]]:
    terms = {}
    num_terms = 0

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for word in page.extract_words():
                    term = CleanupPatterns.cleanup_term(word["text"])
                    if term:
                        terms[term] = terms.get(term, 0) + 1
                        num_terms += 1
    except Exception as e:
        return None, e
    else:
        return {"num_terms": num_terms, "terms": terms}, None


def parse_docx_text(word_bytes: bytes) -> tuple[Optional[dict], Optional[Exception]]:
    terms = {}
    num_terms = 0

    # Based on http://etienned.github.io/posts/extract-text-from-word-docx-simply/
    WORD_NAMESPACE = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    PARA = WORD_NAMESPACE + "p"
    TEXT = WORD_NAMESPACE + "t"

    try:
        with zipfile.ZipFile(word_bytes) as docx:
            tree = XML(docx.read("word/document.xml"))

        for paragraph in tree.iter(PARA):
            for node in paragraph.iter(TEXT):
                if node.text:
                    node_words = node.text.split()
                    for node_word in node_words:
                        term = CleanupPatterns.cleanup_term(node_word)
                        if term:
                            terms[term] = terms.get(term, 0) + 1
                            num_terms += 1
    except Exception as e:
        return None, e
    else:
        return {"num_terms": num_terms, "terms": terms}, None


def parse_doc_text(word_bytes: bytes) -> tuple[Optional[dict], Optional[Exception]]:
    terms = {}
    num_terms = 0

    try:
        with OleFileIO(word_bytes) as doc:
            text = DOCParser(doc).extract_text()
            words = text.split()
            for word in words:
                term = CleanupPatterns.cleanup_term(word)
                if term:
                    terms[term] = terms.get(term, 0) + 1
                    num_terms += 1
    except Exception as e:
        return None, e
    else:
        return {"num_terms": num_terms, "terms": terms}, None


@dataclass
class FilenameTuple:
    material_id: int
    extension: str
    doc_path: str
    bow_path: str

    def __init__(self, doc_filename: str):
        parsed_filename = Path(doc_filename)
        self.material_id = int(parsed_filename.stem)
        self.extension = "".join(parsed_filename.suffixes)
        self.doc_path = os.path.join(constants.MATERIAL_DOWNLOAD_DIR, parsed_filename)
        self.bow_path = os.path.join(
            constants.MATERIAL_BAG_OF_WORDS_DIR, parsed_filename.with_suffix(".json")
        )


class Parsers:
    handlers = {
        ".pdf": parse_pdf_text,
        ".docx": parse_docx_text,
        ".doc": parse_doc_text,
    }
    # .doc file parsing only works with the win32.client, which is optional
    # if it's missing, .doc files are skipped
    # if "win32.client" in sys.modules:
    #     handlers[".doc"] = parse_doc_text

    @classmethod
    def is_extension_supported(cls, extension: str):
        return extension in cls.handlers

    @classmethod
    def parse(cls, extension: str, data: bytes):
        return cls.handlers[extension](data)


async def load_document(tup: FilenameTuple):
    async with aiofiles.open(file=tup.doc_path, mode="rb") as f:
        return await f.read()


def parse_document(tup: FilenameTuple, data: bytes):
    if not Parsers.is_extension_supported(tup.extension):
        return "", "No relevant parser implemented"

    bow_dict, parse_error = Parsers.parse(tup.extension, data)

    if parse_error is not None:
        return "", repr(parse_error)

    return json.dumps(bow_dict), None


async def save_bow(tup: FilenameTuple, bag_of_words: str):
    async with aiofiles.open(file=tup.bow_path, mode="w", encoding="utf-8") as f:
        await f.write(bag_of_words)


async def process_doc(
    tup: FilenameTuple,
    loop: asyncio.AbstractEventLoop,
    executor: concurrent.futures.Executor,
):
    bow_exists = await aiofiles.os.path.exists(tup.bow_path)
    if bow_exists:
        return tup.material_id, True, ""

    doc_data = await load_document(tup)
    # Parsing is CPU intensive, so we queue to launch it in a separate process
    # There will be as many parallel processes as there are available CPUs
    bag_of_words, parse_error = await loop.run_in_executor(
        executor, parse_document, tup, doc_data
    )

    if not bag_of_words:
        if parse_error is not None:
            return tup.material_id, False, parse_error
        return tup.material_id, False, "Produced empty vocabulary"

    await save_bow(tup, bag_of_words)

    return tup.material_id, True, ""


async def build_bags_of_words() -> pd.DataFrame:
    filename_tuples = [
        FilenameTuple(filename)
        for filename in os.listdir(constants.MATERIAL_DOWNLOAD_DIR)
    ]

    loop = asyncio.get_running_loop()
    # Using multi-processing rather than multi-threading for CPU-bound tasks
    with concurrent.futures.ProcessPoolExecutor() as executor:
        try:
            results = await gather_unlimited_concurrency(
                "Building bag-of-words doc vocabularies",
                *(process_doc(tup, loop, executor) for tup in filename_tuples)
            )
        except asyncio.CancelledError:
            executor.shutdown(wait=True, cancel_futures=True)
            raise

    tuples_as_df = pd.DataFrame.from_records(
        results, columns=["Material_ID", "Parsing_OK", "Parsing_Error_Message"]
    )
    return tuples_as_df
