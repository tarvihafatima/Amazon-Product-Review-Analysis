import requests
import gzip
import json
import threading
import os
import logging
import config.Read_Configs as Read_Configs

def extract_json_from_gz(url, output_dir):
    try:
        response = requests.get(url)
        gz_file = response.content

        filename = url.split('/')[-1].replace('.gz', '')
        output_file = os.path.join(output_dir, filename)
        
        with gzip.GzipFile(fileobj=gz_file) as f:
            
            json_data = f.read().decode('utf-8')

        with open(output_file, 'w') as f:
            f.write(json_data)
    except Exception as e:
        print(f"Exception occurred while extracting JSON data: {str(e)}")

def extract_data(object_url, output_dir):

    os.makedirs(output_dir, exist_ok=True)

    def run_extraction(url):

        extract_json_from_gz(url, output_dir)

    threads = []

    thread = threading.Thread(target=run_extraction, args=(object_url,))
    threads.append(thread)
    thread.start()

    for thread in threads:
        thread.join()
