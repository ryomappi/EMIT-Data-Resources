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
sys.path.append("../modules/")
from emit_tools import emit_xarray


def download_from_url(urls, output_path):
    fs = earthaccess.get_requests_https_session()
    for url in urls:
        granule_asset_id = url.split("/")[-1]
        fp = f"{output_path}/{granule_asset_id}"
        if not os.path.exists(fp):
            with fs.get(url, stream=True) as src:
                with open(fp, "wb") as dst:
                    print(f"Downloading {url} to {fp}...")
                    for chunk in src.iter_content(chunk_size=64 * 1024 * 1024):
                        dst.write(chunk)
    return fp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=str, help="GeoJSON data path")
    args = parser.parse_args()

    # earthdataにログイン
    load_dotenv()
    earthaccess.login(strategy="environment", persist=True)

    # GeoJSONデータの読み込み
    geojson_path = Path("../../") / args.json
    print(f"geojson_path: {geojson_path}\n")
    geojson = gpd.read_file(geojson_path)
    geojson.plot()  # GeoJSONデータのプロット
    plt.show()

    # BBOXの取得
    bbox = tuple(list(geojson.total_bounds))

    # 検索パラメータの設定
    start_date = "2024-10-20"
    end_date = "2024-10-21"

    # L2A, L2Bデータの検索
    l2a_results = earthaccess.search_data(
        short_name="EMITL2ARFL", bounding_box=bbox, temporal=(start_date, end_date)
    )
    l2b_results = earthaccess.search_data(
        short_name="EMITL2BCH4ENH", bounding_box=bbox, temporal=(start_date, end_date)
    )

    # stream data
    output_path = "../../data/registration_test"

    l2a_urls = [granule.data_links() for granule in l2a_results][0]
    l2b_urls = [granule.data_links() for granule in l2b_results][0]

    download_from_url(l2a_urls, output_path)
    download_from_url(l2b_urls, output_path)

if __name__ == "__main__":
    main()
