from io import BytesIO
from zipfile import ZipFile
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv('.env')
files_links_paths = os.getenv("files_links_paths")
bronze_layer_path = os.getenv("bronze_layer_path")

with open(files_links_paths) as json_file:
    files_links_paths = json.load(json_file)


def extract_save_files(link, bronze_layer_path, file_name):
    req = requests.get(link)
    z = ZipFile(BytesIO(req.content))
    file = z.extract(z.filelist[1], bronze_layer_path)
    print(file)
    with ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(f"{bronze_layer_path}/{file_name}/")
    os.remove(file)
    os.rmdir('/'.join(file.split('/')[:2]))
    print("Guardado!!")


for file_name, link in files_links_paths.items():
    extract_save_files(link, bronze_layer_path, file_name)
# ---------INICIO DE PIPELINE-------------
# Cargar los archivos brutos
# Convertir a dataframes
# Hacer transformaciones mínimas (si es necesario), convertir a parquet y guardar en bronze_layer
# ---------FIN DE PIPELINE-------------

#####################################################################################################

# ---------INICIO DE PIPELINE 2.0------------------
# Descargar desde internet los archivos
# Cargar en memoria cada zip
# Descomprimir estos archivos
# Cargarlos en AWS s3
# ---------FIN DE PIPELINE 2.0-------------
