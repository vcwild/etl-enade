# Imports the Google Cloud client library
from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "etl-demo-fractal"

# Creates the new bucket
bucket = storage_client.create_bucket(bucket_name, project='etl-demo-297919')

print("Bucket {} created.".format(bucket.name))
