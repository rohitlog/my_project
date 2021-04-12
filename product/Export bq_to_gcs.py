import json
import os
import logging
from google.cloud import bigquery


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config_export1.json')

try:
    with open(CONFIG_PATH,'r') as config_file:
        config = json.load(config_file)
        project_id = config['project_id']
        dataset_id = config['dataset']
        bucket_name = config['bucket_name']
        file_name = config['file_name']
        table_id = config['table_id']
        dataset_manual = config['dataset_manual']
        table_share_market =config['table_share_market']
        share_market_file =config['share_market_file']
        char_list_core_plus_file = config['char_list_core_plus_file']
        char_list_core_plus_table = config['char_list_core_plus_table']

except KeyError as e:
    logging.error("key :{} not found in config.json file.".format(e))


def export_to_gcs(file_name_call, table_id_call):
    file_name = file_name_call
    table_id = table_id_call
    client = bigquery.client()
    destination_uri = "gs://{}/{}".format(bucket_name, file_name)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.job.ExtractJobConfig(print_header=False)
    extract_job = client.extract_table(table_ref,destination_uri, job_config=job_config, location="us",)
    job_id = extract_job.job_id
    print(job_id)
    extract_job.result() #waits for jobs to complete
    print("Exported {}:{}.{} to {}".format(project_id, dataset_id, table_id, destination_uri))
    return "successfully exported table in gcs"

def export_share_market():
    client = bigquery.Client()
    destination_uri = "gs://{}/{}".format(bucket_name, share_market_file)
    print("share market uri is: {}".format(destination_uri))
    dataset_ref = bigquery.DatasetReference(project_id, dataset_manual)
    table_ref = dataset_ref.table(table_share_market)
    job_config = bigquery.job.ExtractJobConfig(print_header=False)
    extract_job = client.extract_table(table_ref, destination_uri, job_config=job_config, location="US",)
    job_id = extract_job.job_id
    print(job_id)
    extract_job.result() #Waits for job to complete.
    print("Exported {}:{}.{} to {}".format(project_id, dataset_manual, table_share_market, destination_uri))
    return "successfully exported table in gcs"

def export_char_list_core_plus():
    client = bigquery.Client()
    destination_uri = "gs://{}/{}".format(bucket_name, char_list_core_plus_file)
    print("share market uri is: {}".format(destination_uri))
    dataset_ref = bigquery.DatasetReference(project_id, dataset_manual)
    table_ref = dataset_ref.table(char_list_core_plus_table)
    job_config = bigquery.job.ExtractJobConfig(print_header=False)
    extract_job = client.extract_table(table_ref, destination_uri, job_config=job_config, location="US")
    job_id = extract_job.job_id
    print(job_id)
    extract_job.result() #Waits for job to complete
    print("Exported {}:{}.{}".format(project_id, dataset_manual, char_list_core_plus_table, destination_uri))
    return "successfully exported table in gcs"


def pipeline():
    for i in range(len(file_name)):
        file_name_call=file_name[i]
        table_id_call=table_id[i]
        print(len(file_name[i]))
        export_to_gcs(file_name_call, table_id_call)

if __name__ == "__main__":
    pipeline()
    export_share_market()
    export_char_list_core_plus()

