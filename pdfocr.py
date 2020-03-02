import os, io
import re
from google.cloud import vision
from google.cloud import storage
from google.protobuf import json_format

"""
# pip install --upgrade google-cloud-storage
"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'Cambridge Five OCR-13374180dee6.json'
client = vision.ImageAnnotatorClient()

batch_size = 2
mime_type = 'application/pdf'
feature = vision.types.Feature(
    type=vision.enums.Feature.Type.DOCUMENT_TEXT_DETECTION)
for name in ['KV 4_466','KV 4_467','KV 4_468','KV 4_469-2','KV 4_469-3','KV 4_469','KV 4_470','KV 4_471','KV 4_472','KV 4_473','KV 4_474','KV 4_475','KV-4-185_1','KV-4-185_2','KV-4-186_1','KV-4-186_2','KV-4-187_1','KV-4-187_2','KV-4-188_1','KV-4-188_2','KV-4-189_1','KV-4-190_1','KV-4-190_2','KV-4-191_1','KV-4-191_2','KV-4-192_1','KV-4-192_2','KV-4-192_3','KV-4-193_1','KV-4-193_2','KV-4-193_3','KV-4-194_1','KV-4-194_2','KV-4-195_1','KV-4-195_2','KV-4-195_3','KV-4-196_1','KV-4-196_2','KV-4-196_3']:
  gcs_source_uri = 'gs://cambridgefivepdfs/wetransfer-5e0e14/' + name + '.pdf'
  gcs_source = vision.types.GcsSource(uri=gcs_source_uri)
  input_config = vision.types.InputConfig(gcs_source=gcs_source, mime_type=mime_type)

  gcs_destination_uri = 'gs://cambridgefivepdfs/results/pdf_result'
  gcs_destination = vision.types.GcsDestination(uri=gcs_destination_uri)
  output_config = vision.types.OutputConfig(gcs_destination=gcs_destination, batch_size=batch_size)

  async_request = vision.types.AsyncAnnotateFileRequest(
      features=[feature], input_config=input_config, output_config=output_config)

  operation = client.async_batch_annotate_files(requests=[async_request])
  operation.result(timeout=180)

  storage_client = storage.Client()
  match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
  bucket_name = match.group(1)
  prefix = match.group(2)
  bucket = storage_client.get_bucket(bucket_name)

  # List object with the given prefix
  plaintext = open(name + '.txt', 'w')
  blob_list = list(bucket.list_blobs(prefix=prefix))
  for blob in blob_list:
      json_string = blob.download_as_string()
      response = json_format.Parse(
              json_string, vision.types.AnnotateFileResponse())
      first_page_response = response.responses[0]
      annotation = first_page_response.full_text_annotation
      plaintext.write(annotation.text)
  plaintext.close
