import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
# The previous problematic import is REMOVED
import csv
import re
import datetime

# Email validation regex
EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$")

def parse_csv(line):
    for row in csv.reader([line]):
        return {
            'customer_id': row[0].strip(),
            'customer_name': row[1].strip(),
            'email': row[2].strip(),
            'signup_date': row[3].strip()
        }

def clean_and_validate(record):
    # Remove rows with missing or invalid customer_id
    try:
        # Check if customer_id can be converted to a positive integer
        customer_id = int(record['customer_id'])
        if customer_id <= 0:
            return None
    except Exception:
        return None
    
    # Remove rows with missing name
    if not record['customer_name']:
        return None
    
    # Validate email
    if not EMAIL_REGEX.match(record['email']):
        return None
        
    # Validate date format
    try:
        datetime.datetime.strptime(record['signup_date'], '%Y-%m-%d')
    except Exception:
        return None
        
    # Return cleaned record
    return {
        'customer_id': customer_id,
        'customer_name': record['customer_name'].title(),
        'email': record['email'].lower(),
        'signup_date': record['signup_date']
    }

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'de-project-479112'
    google_cloud_options.job_name = 'gcs-to-bq-customers-streaming'
    google_cloud_options.staging_location = 'gs://de-project-source/staging/'
    google_cloud_options.temp_location = 'gs://de-project-source/temp/'
    google_cloud_options.region = 'us-central1'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    input_file = 'gs://de-project-source/incoming/customers.csv'
    output_table = 'de-project-479112:customer_data.customers'

    # FIX: Define the schema as a Python list of dictionaries (BigQuery JSON format)
    # This avoids the troublesome 'TableSchema' import while correctly setting 'REQUIRED' mode.
    table_schema = {
        'fields': [
            {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'signup_date', 'type': 'DATE', 'mode': 'NULLABLE'}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(parse_csv)
            | 'Clean & Validate' >> beam.Map(clean_and_validate)
            | 'Filter Invalid' >> beam.Filter(lambda x: x is not None)
            
            # --- Deduplication (Using Tuple for Resilience) ---
            | 'Dict to Tuple' >> beam.Map(
                lambda record: (
                    record['customer_id'], 
                    record['customer_name'], 
                    record['email'], 
                    record['signup_date']
                )
            )
            | 'Deduplicate' >> beam.Distinct()
            | 'Tuple to Dict' >> beam.Map(
                lambda t: {
                    'customer_id': t[0], 
                    'customer_name': t[1], 
                    'email': t[2], 
                    'signup_date': t[3]
                }
            )
            # --- END DEDUPLICATION FIX ---
            
            | 'Write to BQ' >> beam.io.WriteToBigQuery(
                output_table,
                schema=table_schema, # Use the dictionary object
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    run()