# MERLOT material keyword generator

This project downloads MERLOT metadata, queries the links for all the materials, gathers PDFs, Word documents, HTML sites, and plaintext files, and uses TF-IDF to generate keywords for each. It outputs a `results.csv` file, which can be loaded as a pandas DataFrame (or opened in Excel or similar).
The application caches MERLOT data locally to avoid unnecessary load on their servers, running this will create `cache` and `materials` directories.

## Setup

1. Install Python >=3.9.
2. Install project pre-requisites with `pip install -r requirements.txt`.
3. Copy the `.env.example` file to `.env` and make sure to enter a MERLOT Web Services license key into the new `.env` file.

## Running

Run `python src/main.py`.

Beware, downloading just PDF and Word materials will download and store about ~7GB of data. The materials are cached and won't be redownloaded again, unless deleted.

## Caveats

If you use Windows and get errors relating to `python-magic`, you may need to additionally install libmagic binaries: `pip install python-magic-bin`.

For specific MERLOT API documentation, you have to email the MERLOT team, it does not exist on any public site.

The implementation is highly parallelised (because it would be way too slow otherwise). On Windows, there are 2 asynchronous event loop policies: Proactor and Selector. The default one is Proactor -- it does not have a limit on max open sockets at once. With this policy, occasional `ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host` and `OSError: [WinError 10038] An operation was attempted on something that is not a socket` may crop up. With the alternate Selector policy, this error does not occur, but the concurrency needs to be limited in `download.py` and `metadata.py`, where calls to `gather_unlimited_concurrency` need to be replaced with `gather_limited_concurrency(constants.CONCURRENCY_LIMIT, ...)`. These errors are silenced in `main.py`, but a better solution would deal with them.

## Relevant links

[MERLOT Web Service License Request Form](https://www.merlot.org/merlot/signWebServicesForm.htm)
[General MERLOT docs](https://info.merlot.org/merlothelp/topic.htm#t=MERLOT_Technologies.htm%23MERLOT_Web_Services)