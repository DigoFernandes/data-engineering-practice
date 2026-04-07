import os

from pathlib import Path

from urllib.parse import urlparse

import requests

import zipfile


download_uris = [

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",

]

DOWNLOAD_DIR = Path("downloads")



def ensure_dir(dir_path: Path = DOWNLOAD_DIR) -> Path:

    dir_path.mkdir(parents=True, exist_ok=True)

    return dir_path



def main():

    # your code here

    pass



if __name__ == "__main__":

    main()

import os

from pathlib import Path

import requests


download_uris = [

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",

]

BASE_DIR = Path(__file__).resolve().parent

DOWNLOAD_DIR = BASE_DIR / "downloads"



def ensure_dir(path: Path = DOWNLOAD_DIR) -> Path:

    path.mkdir(parents=True, exist_ok=True)

    return path



def uri_filename(uri: str) -> str:

    name = Path(urlparse(uri).path).name

    if not name:

        raise ValueError(f"URI without a name: {uri}")

    return name



def download_zip(uri: str, dest: Path, *, timeout_s: int = 60) -> Path:

    dest.parent.mkdir(parents=True, exist_ok=True)


    with requests.get(uri, stream=True, timeout=timeout_s) as r:

        r.raise_for_status()

        with open(dest, "wb") as f:

            for chunk in r.iter_content(chunk_size=1024 * 256):

                if chunk:

                    f.write(chunk)

    return dest



def extract_csvs(zip_path: Path, out_dir: Path) -> int:

    extracted = 0

    with zipfile.ZipFile(zip_path, "r") as zf:

        for m in zf.infolist():

            if m.is_dir():

                continue

            if m.filename.lower().endswith(".csv"):

                zf.extract(m, path=out_dir)

                extracted += 1

    return extracted



def download_and_extract(uri: str, out_dir: Path = DOWNLOAD_DIR) -> bool:

    ensure_dir(out_dir)

    zip_path = out_dir / uri_filename(uri)

    try:

        download_zip(uri, zip_path)

        extract_csvs(zip_path, out_dir)

        zip_path.unlink(missing_ok=True)

        return True

    except (requests.RequestException, zipfile.BadZipFile, OSError, ValueError):

        zip_path.unlink(missing_ok=True)

        return False



def run_all(uris=download_uris, out_dir: Path = DOWNLOAD_DIR) -> tuple[int, int]:

    ok = 0

    fail = 0

    for uri in uris:

        if download_and_extract(uri, out_dir):

            ok += 1

        else:

            fail += 1

    return ok, fail



def main():

    ensure_dir(DOWNLOAD_DIR)

    ok, fail = run_all()

    print(f"ok = {ok} | fails = {fail} | downloads = {DOWNLOAD_DIR}")



if __name__ == "__main__":

    main()



