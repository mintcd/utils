import requests
from bs4 import BeautifulSoup

import json, csv
import zipfile, rarfile

import re

import threading

class DataStructureProcessing:
    def unique(lst : list):
        unique_elements = []
        seen = set()
        for element in lst:
            # Convert the element to a tuple if it's not hashable
            key = element if isinstance(element, (int, float, str, bool)) else tuple(element)
            if key not in seen:
                    seen.add(key)
                    unique_elements.append(element)
        return unique_elements

    def key_structure(dictionary: dict, indent=0, display=True):
        """
        Recursively traverses through a dictionary and returns its structure.

        Args:
            dictionary (dict): The dictionary to analyze.
            indent (int, optional): The level of indentation for printing. Defaults to 0.
            display (bool, optional): If True, the function prints the key structure. Defaults to True.

        Returns:
            dict: A nested dictionary representing the structure of the input dictionary.

        Examples:
            Consider a dictionary `sample_dict`:
            
            >>> sample_dict = {
            ...     "key1": {
            ...         "key1_1": "value1_1",
            ...         "key1_2": "value1_2"
            ...     },
            ...     "key2": [
            ...         {"key2_1": "value2_1"},
            ...         {"key2_2": "value2_2"}
            ...     ],
            ...     "key3": "value3"
            ... }
            
            Calling `key_structure(sample_dict)` will return the following structure:
            
            >>> {
            ...     "key1": {
            ...         "key1_1": "str",
            ...         "key1_2": "str"
            ...     },
            ...     "key2": [
            ...         {"key2_1": "str"},
            ...         {"key2_2": "str"}
            ...     ],
            ...     "key3": "str"
            ... }

        """
        key_structure_dict = {}
        
        if isinstance(dictionary, dict):
            for key, value in dictionary.items():
                key_structure_dict[key] = {}
                if isinstance(value, dict):
                    key_structure_dict[key] = DataStructureProcessing.key_structure(value, indent + 1, False)
                elif isinstance(value, list) and value:
                    key_structure_dict[key] = [DataStructureProcessing.key_structure(value[0], indent + 1, False)]
                else:
                    key_structure_dict[key] = type(value).__name__
        elif isinstance(dictionary, list) and dictionary:
            element = dictionary[0]
            if isinstance(element, dict) or (isinstance(element, list) and element):
                key_structure_dict = [DataStructureProcessing.key_structure(element, indent + 1, False)]

        if display: 
            print(json.dumps(key_structure_dict, indent=4))
        
        return key_structure_dict


    def splitted_words(text):
        pattern = r"\b\w+(?:'\w+)?\b|\w+"
        return re.findall(pattern, text)
class FileProcessing:
    def read_json_from_archive(archive_path, json_file_name):
        """
        Function to read a JSON file from a ZIP or RAR archive. Note: there are bugs with rarfile library, which makes this function unable to deal with rar files.

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

class WorkloadProcessing:
    def parallel_processing(job, arg_list, num_threads=8):
        threads = []
        result = []

        """
        Function to thread over a replicated job

        Parameters:
            job (func): a function that takes each argument in arg_list to be parameter(s).
            arg_list (list).
            num_threads (number): Defaults to 8.

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

        print(f"Initiated {num_threads} thread(s).")

        # Wait for all threads to complete
        for t in threads:
            t.join()

        return result