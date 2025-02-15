import time
import earthaccess
import geopandas as gpd
import sys
from dotenv import load_dotenv
import argparse
from pathlib import Path
import concurrent.futures
import re

MAX_WORKERS = 8
DOWNLOAD_TIMEOUT = 1200


def download_from_url(geojson_id, urls, output_path):
    fs = earthaccess.get_requests_https_session()

    if any(output_path.glob(f"{geojson_id}_*")):
        print(f"File for {geojson_id} already exists, skipping download.")
        return
    for url in urls:
        granule_asset_id = url.split("/")[-1]
        fp = output_path / f"{geojson_id}_{granule_asset_id}"
        try:
            with fs.get(url, stream=True) as src:
                with fp.open("wb") as dst:
                    print(f"Downloading {url} to {fp}...")
                    download_start = time.time()
                    for chunk in src.iter_content(chunk_size=64 * 1024 * 1024):
                        if time.time() - download_start > DOWNLOAD_TIMEOUT:
                            raise TimeoutError(
                                f"Download of {url} timed out after {DOWNLOAD_TIMEOUT} seconds."
                            )
                        dst.write(chunk)
                    print(f"Download of {url} completed.")
        except Exception as e:
            print(f"{geojson_id}: Error downloading {url}: {e}. Skipping this file.")
            if fp.exists():
                fp.unlink()
            continue
    return fp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json", type=str, default="data/dataset/geojsons", help="GeoJSON data dir"
    )
    args = parser.parse_args()

    # earthdataにログイン
    load_dotenv()
    earthaccess.login(strategy="environment", persist=True)

    geojson_dir = Path(args.json)
    if not geojson_dir.exists():
        print(f"GeoJSON directory {geojson_dir} does not exist.")
        sys.exit(1)

    # 出力ディレクトリの設定
    output_path_l2a = Path("data/dataset/l2a")
    output_path_l2b = Path("data/dataset/l2b")
    output_path_l2a.mkdir(parents=True, exist_ok=True)
    output_path_l2b.mkdir(parents=True, exist_ok=True)

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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
            end_date = "2024-09-30"

            # L2A, L2Bデータの検索
            l2a_results = earthaccess.search_data(
                short_name="EMITL2ARFL",
                bounding_box=bbox,
                temporal=(start_date, end_date),
            )
            l2b_results = earthaccess.search_data(
                short_name="EMITL2BCH4PLM",
                bounding_box=bbox,
                temporal=(start_date, end_date),
            )

            # L2AとL2Bで同じタイムスタンプを持つペアが存在するか確認する
            l2a_url = None
            l2a_timestamp = None
            valid_granule_b = None

            if l2a_results and l2b_results:
                found_pair = False
                for granule in l2a_results:
                    links = granule.data_links()
                    # 「EMIT_L2A_RFL_」を含み、不要な文字列が含まれていないリンクを抽出
                    valid_links = [
                        link for link in links
                        if "EMIT_L2A_RFL_" in link
                        and "EMIT_L2A_RFLUNCERT" not in link
                        and "EMIT_L2A_MASK" not in link
                    ]
                    if valid_links:
                        candidate_url = valid_links[0]
                        m = re.search(r'_(\d{8}T\d{6})_', candidate_url)
                        if m:
                            candidate_ts = m.group(1)
                            # L2Bの中からタイムスタンプが同じgranuleを探す
                            for granule_b in l2b_results:
                                for link_b in granule_b.data_links():
                                    m2 = re.search(r'_(\d{8}T\d{6})_', link_b)
                                    if m2 and m2.group(1) == candidate_ts:
                                        l2a_url = candidate_url
                                        l2a_timestamp = candidate_ts
                                        valid_granule_b = granule_b
                                        found_pair = True
                                        break
                                if found_pair:
                                    break
                    if found_pair:
                        break

                if not found_pair:
                    print(f"{geojson_id}: L2AとL2Bで同じタイムスタンプのペアが見つかりませんでした。")
                    continue  # 次のgeojsonへ
            else:
                print("L2AもしくはL2Bのデータが見つかりませんでした。")
                continue

            # ペアが存在する場合、L2A, L2Bのダウンロードを実行
            print(f"L2A timestamp: {l2a_timestamp}")
            futures.append(
                executor.submit(
                    download_from_url, geojson_id, [l2a_url], output_path_l2a
                )
            )

            # L2B: valid_granule_b から先頭の有効なリンクを利用
            links_b = valid_granule_b.data_links()
            if links_b and len(links_b) > 0:
                # 必要に応じてL2B用にフィルタ条件を追加してください
                l2b_url = links_b[0]
                futures.append(
                    executor.submit(
                        download_from_url, geojson_id, [l2b_url], output_path_l2b
                    )
                )
            else:
                print("有効なL2B URLが見つかりませんでした。")

    concurrent.futures.wait(futures)
    print("All downloads completed.")


if __name__ == "__main__":
    main()
