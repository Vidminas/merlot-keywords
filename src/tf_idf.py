from collections import namedtuple
from operator import itemgetter
import aiofiles
import aiofiles.os
import pandas as pd
import numpy as np
import json
import os

import constants
from datatypes import MERLOTMaterial
from utils import gather_unlimited_concurrency


async def build_corpus_vocabulary():
    async def load_doc_vocab(filename):
        async with aiofiles.open(
            file=os.path.join(constants.MATERIAL_BAG_OF_WORDS_DIR, filename),
            mode="r",
            encoding="utf-8",
        ) as f:
            data = await f.read()
            return json.loads(data)

    vocabs = await gather_unlimited_concurrency(
        "Building corpus vocabulary",
        *(
            load_doc_vocab(filename)
            for filename in os.listdir(constants.MATERIAL_BAG_OF_WORDS_DIR)
        ),
    )

    # The loops below are synchronous to avoid race conditions when modifying
    # the doc_frequency or term_frequency dict

    doc_frequency = {}
    for vocab in vocabs:
        for word in vocab["terms"]:
            doc_frequency[word] = doc_frequency.get(word, 0) + 1

    output = json.dumps({"num_docs": len(vocabs), "doc_frequency": doc_frequency})
    async with aiofiles.open(
        file=constants.CORPUS_INVERSE_VOCABULARY_PATH, mode="w", encoding="utf-8"
    ) as f:
        await f.write(output)

    term_frequency = {}
    for vocab in vocabs:
        for word in vocab["terms"]:
            term_frequency[word] = term_frequency.get(word, 0) + vocab["terms"][word]

    output = json.dumps({"num_terms": len(term_frequency), "terms": term_frequency})
    async with aiofiles.open(
        file=constants.CORPUS_VOCABULARY_PATH, mode="w", encoding="utf-8"
    ) as f:
        await f.write(output)


async def load_stopwords():
    async with aiofiles.open(
        file=constants.CORPUS_VOCABULARY_PATH, mode="r", encoding="utf-8"
    ) as f:
        data = await f.read()

    corpus_vocab = json.loads(data)
    ranked_terms: list[tuple[str, int]] = sorted(
        corpus_vocab["terms"].items(), key=itemgetter(1), reverse=True
    )
    top_terms = [
        term
        for (idx, (term, _)) in enumerate(ranked_terms)
        if idx < constants.NUM_STOP_WORDS
    ]
    return set(top_terms)


def compute_tf_idf(global_vocab: dict, local_vocab: dict):
    terms = local_vocab["terms"].keys()

    # TF(t) = (Number of times term t appears in a document) / (Total number of terms in the document)
    tf = {t: local_vocab["terms"][t] / local_vocab["num_terms"] for t in terms}

    # IDF(t) = log_e(Total number of documents / Number of documents with term t in it)
    idf = {
        t: np.log(global_vocab["num_docs"] / global_vocab["doc_frequency"][t])
        for t in terms
    }

    return {t: tf[t] * idf[t] for t in terms}


def generate_keywords(global_vocab: dict, stop_words: set[str], local_vocab: dict):
    tf_idf = sorted(
        compute_tf_idf(global_vocab, local_vocab).items(),
        key=itemgetter(1),
        reverse=True,
    )

    keywords = []
    for term, score in tf_idf:
        if term in stop_words:
            continue
        if score >= constants.TF_IDF_SCORE_THRESHOLD:
            keywords.append(term)
            if len(keywords) >= constants.NUM_MAX_KEYWORDS:
                break
        else:
            break

    return keywords


async def get_keywords_for_row(
    corpus_vocab: dict, stop_words: set[str], row: namedtuple
):
    material_id = str(row.Material_ID)
    material_vocab_path = os.path.join(
        constants.MATERIAL_BAG_OF_WORDS_DIR, material_id + ".json"
    )

    vocab_exists = await aiofiles.os.path.exists(material_vocab_path)
    if not vocab_exists:
        return ""

    async with aiofiles.open(file=material_vocab_path, mode="r", encoding="utf-8") as f:
        data = await f.read()

    doc_vocab = json.loads(data)

    kws = generate_keywords(corpus_vocab, stop_words, doc_vocab)
    return ", ".join(kws)


async def generate_and_save_keywords_csv(
    materials: list[MERLOTMaterial], parsing_info: pd.DataFrame, stop_words: set[str]
):
    with open(
        file=constants.CORPUS_INVERSE_VOCABULARY_PATH, mode="r", encoding="utf-8"
    ) as f:
        corpus_vocab: dict = json.load(f)

    kws = await gather_unlimited_concurrency(
        "Generating keywords",
        *(
            get_keywords_for_row(corpus_vocab, stop_words, row)
            for row in parsing_info.itertuples(index=True, name="Material")
        ),
    )

    materials_dict = {material.materialid: material for material in materials}

    metadata_title = parsing_info.apply(
        lambda row: materials_dict[row["Material_ID"]].title, axis=1
    )
    metadata_keywords = parsing_info.apply(
        lambda row: materials_dict[row["Material_ID"]].keywords, axis=1
    )

    extended_df = parsing_info.assign(
        Metadata_Title=metadata_title,
        Metadata_Keywords=metadata_keywords,
        Generated_Keywords=kws,
    )
    extended_df.set_index("Material_ID", inplace=True)
    extended_df.to_csv(constants.TF_IDF_DATA_PATH, escapechar="\\")
