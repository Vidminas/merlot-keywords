import platform
import asyncio
from dotenv import load_dotenv
from metadata import (
    download_merlot_metadata,
    load_merlot_metadata,
    guess_all_filetypes,
    test_all_urls,
    save_material_info_csv,
    load_material_info_csv,
    save_broken_urls_csv,
    save_mismatched_filetypes_csv,
)
from download import download_materials
from bag_of_words import build_bags_of_words
from tf_idf import (
    build_corpus_vocabulary,
    load_stopwords,
    generate_and_save_keywords_csv,
)


async def main():
    load_dotenv(verbose=True)

    # Method calls that take more than 1 minute are identified below

    # await download_merlot_metadata()  # Takes about 30min

    materials = load_merlot_metadata()  # Takes about 1min

    # material_urls = await test_all_urls(materials)  # Takes about 1.5h
    # material_filetypes = await guess_all_filetypes(material_urls)  # Takes about 30min
    # save_material_info_csv(material_urls, material_filetypes)

    # material_info = load_material_info_csv()
    # material_info = await download_materials(material_info)  # Takes about 30min on first run, < 1min thereafter
    # save_broken_urls_csv(material_info) 
    # save_mismatched_filetypes_csv(material_info)

    parsing_info = await build_bags_of_words()  # Takes about 2h on first run, < 1min thereafter
    await build_corpus_vocabulary()
    stop_words = await load_stopwords()
    await generate_and_save_keywords_csv(materials, parsing_info, stop_words)  # Takes about 1 min


if __name__ == "__main__":
    # A bug in aiohttp causes "RuntimeError: Event loop is closed" with the Proactor policy
    # https://github.com/aio-libs/aiohttp/issues/4324#issuecomment-676675779
    if platform.system() == "Windows":
        # a simple option would be to use the alternative Selector policy
        # but the alternative Selector policy limits concurrency
        # https://stackoverflow.com/questions/47675410/python-asyncio-aiohttp-valueerror-too-many-file-descriptors-in-select-on-win
        # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        from functools import wraps

        from asyncio.proactor_events import _ProactorBasePipeTransport

        def silence_event_loop_closed(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                try:
                    return func(self, *args, **kwargs)
                except RuntimeError as e:
                    if str(e) != 'Event loop is closed':
                        raise
            return wrapper

        # This is my own addition, to silence these 2 errors:
        # ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host
        # OSError: [WinError 10038] An operation was attempted on something that is not a socket
        # Which occur in _ProactorBasePipeTransport._call_connection_lost
        # called by the close() method
        def silence_connection_lost(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                try:
                    return func(self, *args, **kwargs)
                except ConnectionResetError as e:
                    if e.winerror != 10054:
                        raise
                except OSError as e:
                    if e.winerror != 10038:
                        raise
            return wrapper

        _ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)
        _ProactorBasePipeTransport.close = silence_connection_lost(_ProactorBasePipeTransport.close)

    asyncio.run(main())
