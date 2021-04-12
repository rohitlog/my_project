import json
import os
import logging

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(PROJECT_ROOT,'Config_json.json')

try:
    with open(CONFIG_PATH,'r') as config_file:
        config = json.load(config_file)
        file_name = config['File_name']
        table_id = config['table_id']

except KetError as e:
    logging.error("key :{} not found in config.json file.".format(e))

def pipeline():
    for i in range(len(file_name)):
        file_name_call = file_name[i]
        table_id_call = table_id[i]
        print(file_name[i])
if __name__ == "__main__":
    pipeline()