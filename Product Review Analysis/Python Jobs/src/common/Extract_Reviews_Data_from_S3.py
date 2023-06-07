# Import Dependencies

import requests
import gzip
import threading
import os


def extract_json_from_gz(url, output_dir):

    # Fucntion to Extract JSON data from gz file

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

    # Function for Extraction in Threads
    
    try:
        os.makedirs(output_dir, exist_ok=True)

        def run_extraction(url):

            extract_json_from_gz(url, output_dir)

        threads = []

        thread = threading.Thread(target=run_extraction, args=(object_url,))
        threads.append(thread)
        thread.start()

        for thread in threads:
            thread.join()

    except Exception as e:
        print(f"Exception occurred while extracting JSON data: {str(e)}")
