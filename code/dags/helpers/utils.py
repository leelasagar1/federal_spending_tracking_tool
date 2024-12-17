import re

def get_latest_files(files):

    pattern = r'usa_spending_data/[a-z_]+_(\d{8}_\d{6})\.csv'

    # Extract all unique timestamps
    timestamps = set()
    for file in files:
        match = re.search(pattern, file)
        if match:
            timestamps.add(match.group(1))

    # Find the latest timestamp
    latest_timestamp = max(timestamps)

    # Find all files with the latest timestamp
    latest_files = [file for file in files if latest_timestamp in file]

    # Output the list of latest files
    return latest_files

def find_file_by_keyword(file_list, keyword):
    """
    This function takes a list of file paths and a keyword, and returns the file(s) that contain the keyword.

    :param file_list: List of file paths
    :param keyword: Keyword to search for in the file names
    :return: List of files matching the keyword or None if no matches found
    """
    keyword = keyword.lower()
    for file in file_list:
        if keyword in file.lower():
            return file

    
    return None