# Import Dependencies

import os
import gzip
import io


def rename_file_extension(directory):

    # Rename FileNames
    
    old_extension = ".gz2"
    new_extension = ".json"

    # loop through all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(old_extension):

        # construct the new file name with the updated extension
            new_filename = os.path.splitext(filename)[0] + new_extension
        
            # rename the file
            os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))


def decompress_file(gz_path, dest_folder,file_name, chunk_size=1024*1024*1024*4):
    try:
        # Create the destination folder if it doesn't exist
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
        
        # Construct the destination file path
        dest_path = os.path.join(dest_folder, file_name[:-3])

        # Open the gzipped file and read its contents in chunks
        with gzip.GzipFile(gz_path, 'rb') as gz_file, open(dest_path, 'w') as json_file:
            buffer = io.BufferedReader(gz_file, chunk_size)
            while True:
                chunk = buffer.read(chunk_size)
                if not chunk:
                    break
                decoded_chunk = chunk.decode('utf-8')
                json_file.write(decoded_chunk)

        print(f'{file_name} extracted to {dest_path}')
        rename_file_extension(dest_folder)
    except Exception as e:
        print(f'Error occurred while uncompressing file: {e}')