import os
import requests
import gzip
import fasttext
from io import BytesIO
from warcio.archiveiterator import ArchiveIterator
from multiprocessing import Pool, cpu_count
import indices
from os import listdir
from os.path import isfile, join


FASTTEXT_MODEL_PATH = "lid.176.ftz"
WET_BASE_URL = "https://data.commoncrawl.org/"
OUTPUT_DIR = "mongolian_text"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def init_worker():
    global lang_model
    lang_model = fasttext.load_model(FASTTEXT_MODEL_PATH)

def is_mongolian(text, threshold=0.8):
    if not text.strip():
        return False
    try:
        prediction = lang_model.predict(text.strip().replace("\n", " ")[:500])[0][0]
        confidence = lang_model.predict(text.strip().replace("\n", " ")[:500])[1][0]
        return prediction == "__label__mn" and confidence >= threshold
    except Exception:
        print("got error!")
        return False

def process_wet_file(wet_path):
    url = WET_BASE_URL + wet_path.strip()
    print(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            print(f"Failed to download: {url}")
            return

        gzip_file = gzip.GzipFile(fileobj=BytesIO(response.content))

        for record in ArchiveIterator(gzip_file):
            if record.rec_type == 'conversion':
                content = record.content_stream().read().decode('utf-8', errors='ignore')
                if is_mongolian(content):
                    # Save Mongolian text
                    doc_id = record.rec_headers.get_header('WARC-TREC-ID') or record.rec_headers.get_header('WARC-Record-ID').strip("<>")
                    filename = os.path.join(OUTPUT_DIR, f"{doc_id}.txt")
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write(content)
    except Exception as e:
        print(f"Error processing {wet_path}: {e}")

def get_wet_paths(index_name):
    base_url = f'https://data.commoncrawl.org/crawl-data/{index_name}/wet.paths.gz'
    local_path = f'{index_name}_wet.paths.gz'
    response = requests.get(base_url, stream=True)
    if response.status_code != 200:
        print(f"failed to download {index_name}")
        return None
    with open(f"wet_files/{local_path}", 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {local_path}")
    return local_path

def main():
    onlyfiles = [f for f in listdir('wet_files') if isfile(join('wet_files', f))]
    for file in onlyfiles:
        with open("wet.paths", "r") as f:
            wet_files = [line.strip() for line in f if line.strip()]

        print(f"{len(wet_files)} urls need to be proccesed!")

        num_workers = min(cpu_count(), 8)  # Use up to 8 cores
        print(f"Starting with {num_workers} parallel workers...")

        with Pool(processes=num_workers, initializer=init_worker) as pool:
            pool.map(process_wet_file, wet_files)

if __name__ == "__main__":

    for idx in indices.get_commoncrawl_indexes():
        wet_file = get_wet_paths(idx)
    
    main()