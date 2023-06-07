# Import Dependencies

import requests
import os
import threading


# Download File by Parts

def download_file_part(url, file_path, start_byte, end_byte):

    try:
        # send HTTP GET request to download the file
        response = requests.get(url, headers={'Range': f'bytes={start_byte}-{end_byte-1}'}, stream=True)
        response.raise_for_status()

        # open file for writing in binary mode
        with open(file_path, 'r+b') as file:

            # move file pointer to the starting position of the part
            file.seek(start_byte)

            # write the file contents in parts to improve performance
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

    except (requests.exceptions.RequestException, IOError) as e:
        print(f'Error occurred while downloading file chunk: {e}')


def download_file_in_threads(url, destination_folder, chunk_size):
    
    try:
        # create destination folder if it doesn't exist

        file_name = os.path.basename(url)
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        # get file size to calculate number of chunks

        response = requests.head(url)
        response.raise_for_status()
        file_size = int(response.headers.get('content-length', 0))

        # calculate chunk size and number of threads
        chunk_size = 1024 * 1024 * 1024 * chunk_size
        num_threads = file_size // chunk_size + 1

        # create file with zeros to ensure that file size matches content length
        file_path = os.path.join(destination_folder, file_name)
        with open(file_path, 'w+b') as file:
            file.write(b'\0' * file_size)

        # create threads to download file in chunks
        threads = []
        for i in range(num_threads):
            start_byte = i * chunk_size
            end_byte = min((i + 1) * chunk_size, file_size)
            thread = threading.Thread(target=download_file_part, args=(url, file_path, start_byte, end_byte))
            thread.start()
            threads.append(thread)

        # wait for all threads to finish
        for thread in threads:
            thread.join()
            
        print(f'{file_name} downloaded to {destination_folder}')

    except (requests.exceptions.RequestException, IOError) as e:
        print(f'Error occurred while downloading file: {e}')

