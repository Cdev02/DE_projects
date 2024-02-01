import requests
from bs4 import BeautifulSoup
import json
from dotenv import load_dotenv
import os
load_dotenv('.env')

header = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
}

url = os.getenv("files_url")
links_file = os.getenv("files_links_paths")


raw_data = requests.get(url, headers=header)
soup = BeautifulSoup(raw_data.text, "html.parser")

data_items_container = soup.find(
    class_='resources data-files')
data_items_list = data_items_container.find_all(
    'div', {"data-file-type": "microdata"})

files_names = []
files_download_links = []

for d_item in data_items_list:
    file_input = d_item.find(
        'div', class_="resource-left-col").find('input').attrs
    files_names.append(file_input["title"].rstrip('.zip'))
    files_download_links.append(file_input["onclick"].split(',')[
                                1].lstrip(f" '").rstrip(f" ');"))

files_object = dict((f_name, f_link)
                    for f_name, f_link in zip(files_names, files_download_links))

with open(links_file, "w") as outfile:
    json.dump(files_object, outfile)
