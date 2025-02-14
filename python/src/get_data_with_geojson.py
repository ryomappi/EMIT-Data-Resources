import os
import earthaccess
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import xarray as xr
import sys
from dotenv import load_dotenv
import argparse
from pathlib import Path
import concurrent.futures
import re

sys.path.append("python/modules/")
from emit_tools import emit_xarray


def download_from_url(geojson_id, urls, output_path):
    fs = earthaccess.get_requests_https_session()
    for url in urls:
        granule_asset_id = url.split("/")[-1]
        fp = f"{output_path}/{geojson_id}_{granule_asset_id}"
        if os.path.exists(fp):
            print(f"File {fp} already exists, skipping download.")
            continue
        with fs.get(url, stream=True) as src:
            with open(fp, "wb") as dst:
                print(f"Downloading {url} to {fp}...")
                for chunk in src.iter_content(chunk_size=64 * 1024 * 1024):
                    dst.write(chunk)
    return fp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=str, default="data/dataset/geojsons", help="GeoJSON data dir")
    args = parser.parse_args()

    # earthdataにログイン
    load_dotenv()
    earthaccess.login(strategy="environment", persist=True)

    geojson_dir =  Path(args.json)
    if not geojson_dir.exists():
        print(f"GeoJSON directory {geojson_dir} does not exist.")
        sys.exit(1)

    # 出力ディレクトリの設定
    output_path_l2a = "data/dataset/l2a"
    output_path_l2b = "data/dataset/l2b"
    Path(output_path_l2a).mkdir(parents=True, exist_ok=True)
    Path(output_path_l2b).mkdir(parents=True, exist_ok=True)

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for geojson_path in geojson_dir.glob("*.json"):
            print(f"\nProcessing {geojson_path} ...")
            try:
                geojson = gpd.read_file(geojson_path)
            except Exception as e:
                print(f"Error reading {geojson_path}: {e}")
                continue

            # geojson_idの取得
            geojson_id = re.sub(r"\.json$", "", geojson_path.name)

            # BBOXの取得
            bbox = tuple(list(geojson.total_bounds))

            # 検索パラメータの設定
            start_date = "2023-10-01"
            end_date = "2024-10-31"

            # L2A, L2Bデータの検索
            l2a_results = earthaccess.search_data(
                short_name="EMITL2ARFL", bounding_box=bbox, temporal=(start_date, end_date)
            )
            l2b_results = earthaccess.search_data(
                short_name="EMITL2BCH4PLM", bounding_box=bbox, temporal=(start_date, end_date)
            )

            # 最初のgranuleの最初のURLのみをダウンロードするように変更
            if l2a_results and len(l2a_results) > 0:
                granule = l2a_results[0]
                links = granule.data_links()
                if links and len(links) > 0:
                    l2a_url = links[0]
                    futures.append(executor.submit(download_from_url, geojson_id, [l2a_url], output_path_l2a))
                else:
                    print("No URL found for L2A data in the first granule.")
            else:
                print("No L2A data found.")

            if l2b_results and len(l2b_results) > 0:
                granule = l2b_results[0]
                links = granule.data_links()
                if links and len(links) > 0:
                    l2b_url = links[0]
                    futures.append(executor.submit(download_from_url, geojson_id, [l2b_url], output_path_l2b))
                else:
                    print("No URL found for L2B data in the first granule.")
            else:
                print("No L2B data found.")

    concurrent.futures.wait(futures)
    print("All downloads completed.")

if __name__ == "__main__":
    main()
