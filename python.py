import requests
from bs4 import BeautifulSoup

import json, csv
import zipfile, rarfile

import re

import threading



def print_key_structure(dictionary, indent=0):
    """
    Print the key structure of a dictionary
    """
    if isinstance(dictionary, dict):  
        for key, value in dictionary.items():
            print('  ' * indent + str(key))
            if isinstance(value, dict):
                print_key_structure(value, indent + 1)
            elif isinstance(value, list) and value:
                element = value[0]
                if isinstance(element, dict) or (isinstance(element, list) and element):
                    print('  ' * (indent + 1) + "[")
                    print_key_structure(value[0], indent + 2)
                    print('  ' * (indent + 1) + "]")
    elif isinstance(dictionary, list) and dictionary:
        element = dictionary[0]
        if isinstance(element, dict) or (isinstance(element, list) and element):
            print('  ' * indent + "[")
            print_key_structure(element, indent + 1)
            print('  ' * indent + "]")
    else:
        return

def read_json_from_archive(archive_path, json_file_name):
    """
    Function to read a JSON file from a ZIP or RAR archive.

    Parameters:
        archive_path (str): Path to the archive file.
        json_file_name (str): Name of the JSON file within the archive.

    Returns:
        (dict) Dictionary containing the JSON data.
    """
    # Check if it's a ZIP file
    if zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            with zip_ref.open(json_file_name) as json_file:
                json_data = json.load(json_file)
    # Check if it's a RAR file
    elif rarfile.is_rarfile(archive_path):
        print("Reading a RAR file")
        with rarfile.RarFile(archive_path, 'r') as rar_ref:
            for file_in_rar in rar_ref.infolist():
                if file_in_rar.filename == json_file_name:
                    with rar_ref.open(file_in_rar) as json_file:
                        json_data = json.load(json_file)
                        break
            else:
                raise FileNotFoundError(f"{json_file_name} not found in the RAR archive.")
    else:
        raise ValueError("Unsupported archive format. Only ZIP and RAR are supported.")

    return json_data
    print(f"Error reading JSON from archive: {e}")
    return None

def split_words(text):
    pattern = r"\b\w+(?:'\w+)?\b|\w+"
    return re.findall(pattern, text)

def parallel_processing(job, arg_list, num_threads=8):
    threads = []
    result = []

    """
    Function to thread a replicated job

    Parameters:
        job: a function that takes each argument in arg_list to be parameter(s)
        arg_list (list)
        num_threads (number)

    Returns:
        (list) collected results of the threads 
    """

    # Define a function to distribute arguments to threads
    def distribute_args(thread_index, args):
        for i in range(thread_index, len(args), num_threads):
            result.append(job(args[i]))

    # Create and start threads
    for i in range(num_threads):
        t = threading.Thread(target=distribute_args, args=(i, arg_list))
        threads.append(t)
        t.start()

    print("Thread initiated.")

    # Wait for all threads to complete
    for t in threads:
        t.join()

    return result