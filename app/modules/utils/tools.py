import csv
import os
import zipfile


def csv_writer(save_path, data):
    """Write 2D array to .csv file by row"""
    with open(save_path, 'w') as file:
        writer = csv.writer(file)
        for line in data:
            writer.writerow(line)


def csv_pull_key(csv_path, key_index):
    """Pull key from .csv file to array by index"""
    with open(csv_path, 'r') as data_file:
        reader = csv.reader(data_file, delimiter=',')
        keys = [str(col[key_index]) for col in reader]
    return keys


def text_writer(save_path, data):
    """Writes data to .txt file"""
    with open(save_path, 'w') as file:
        for item in data:
            file.write("{}\n".format(item))


def zip_directory(filename, path):
    zipf = zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(path):
        for file in files:
            zipf.write(os.path.join(root, file))
    zipf.close()
