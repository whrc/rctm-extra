from pathlib import Path
from google.cloud import storage


def get_storage_client():
    return storage.Client(project="rangelands-explo-1571664594580")


def upload_directory(storage_client, bucket_name, relative_to, source_directory, destination_directory):
    bucket = storage_client.bucket(bucket_name)

    local_batch_dir = Path(source_directory)
    for local_file in local_batch_dir.rglob("*"):
        if local_file.is_file():
            relative_path = local_file.relative_to(relative_to)
            blob_path = f"{destination_directory}/{relative_path}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(str(local_file))


def download_blob(storage_client, bucket_name, source_blob_name, destination_file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    return destination_file_name


def list_blobs(storage_client, bucket_name, prefix):
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    files = []
    for blob in blobs:
        files.append(blob.name)

    return files