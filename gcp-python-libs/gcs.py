from google.cloud import storage

class GCSClient:
    def __init__(self, project):
        self.client = storage.Client(project)
    def upload_from_string(self, data, bucket_name, blob_name):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data)
    def download_as_string(self, bucket_name, blob_name):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_string()
