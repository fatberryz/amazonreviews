import pandas as pd
import re
from google.cloud import bigquery
import time


def create_bq_client(service_account_credential_path):
	"""
	This function creates a bigquery client which can execute all the required functions. It takes
	in a parameter of the path of where the service account credential file is stored
	"""
	client = bigquery.Client.from_service_account_json(service_account_credential_path)
	return client


def upload_csv(service_account_credential_path, project_name, target_dataset, item_type, csv_file_path):
	"""
	This function uploads a csv file into a GBQ table with schemas already defined. Hence,
	no need for specifying of schema or auto-detecting from CSV

	Parameters:
		project_name: GBQ project name (can be seen from console)
		target_dataset: name of dataset in GBQ
		item_type: "reviews", "products" or "profiles" (determines which table to upload into)
		csv_file_path: path of csv to be uploaded
	"""
	table_id = f"{target_dataset}.{item_type}"

	## Define parameters for load job
	job_config = bigquery.LoadJobConfig(
	#append, empty (only write if doesn't exist) or truncate (overwrite)
	write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
	source_format=bigquery.SourceFormat.CSV,
	skip_leading_rows=1,
	autodetect = False,
	allow_quoted_newlines = True
	)

	client = create_bq_client(service_account_credential_path)

	# uploading of csv file
	with open(csv_file_path, "rb") as source_file:
		job = client.load_table_from_file(source_file, table_id, job_config=job_config)
		# Wait for the load job to complete.
		return job.result()
