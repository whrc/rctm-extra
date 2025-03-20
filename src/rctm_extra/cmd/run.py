from google.cloud import storage
import concurrent.futures
import os
import subprocess
from typing import Tuple, List

from rctm_extra.cmd.base import BaseCommand


class RunCommand(BaseCommand):
    def __init__(self, args):
        super().__init__(args)

    @staticmethod
    def _download_blob(args: Tuple[str, str, str]) -> None:
        """
        Download a single blob from GCP bucket.
        Args:
            args: Tuple containing (bucket_name, source_blob_name, destination_file_name)
        """
        bucket_name, source_blob_name, destination_file_name = args

        # Use global storage_client to avoid creating new connections
        storage_client = storage.Client(project="rangelands-explo-1571664594580")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)


    def _parallel_download_blobs(
        self,
        bucket_name: str,
        file_pairs: List[Tuple[str, str]],
        work_directory: str,
        prefix: str,
        max_workers: int = 4
    ) -> None:
        """
        Download multiple blobs in parallel from GCP bucket.
        
        Args:
            bucket_name: Name of the GCP bucket
            file_pairs: List of tuples containing (config_file, slurm_file) pairs
            work_directory: Local directory to save files
            prefix: Prefix to remove from file paths
            max_workers: Maximum number of concurrent downloads
        """

        download_tasks = []

        for config_file, slurm_file in file_pairs:

            config_raw_file = config_file.replace(f"{prefix}/", "")
            config_dest = os.path.join(work_directory, config_raw_file)
            download_tasks.append((bucket_name, config_file, config_dest))

            slurm_raw_file = slurm_file.replace(f"{prefix}/", "")
            slurm_dest = os.path.join(work_directory, slurm_raw_file)
            download_tasks.append((bucket_name, slurm_file, slurm_dest))

        print("Downloading blobs from the bucket. This may take a while...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self._download_blob, download_tasks)

    def _list_blobs(self, bucket_name, prefix):
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

        files = []
        for blob in blobs:
            files.append(blob.name)

        return files

    def _get_batch_dirs(self, file_list, prefix):
        batch_dirs = []
        for file in file_list:
            file = file.replace(f"{prefix}/", "")
            files = file.split("/")
            batch_dir = files[0]
            batch_dirs.append(batch_dir)

        batch_dirs = set(batch_dirs)
        return batch_dirs

    def execute(self):
        bucket_name = self.args.bucket_name
        prefix = self.args.remote_batch_path
        work_directory = self.args.local_batch_path

        os.makedirs(work_directory, exist_ok=True)

        files = self._list_blobs(bucket_name, prefix)
        batch_dirs = self._get_batch_dirs(files, prefix)

        for batch_dir in batch_dirs:
            full_path = os.path.join(work_directory, batch_dir)
            os.makedirs(full_path, exist_ok=True)

        config_files = []
        slurm_files = []
        for file in files:
            if "config.yaml" in file:
                config_files.append(file)

            if "slurm_runner.sh" in file:
                slurm_files.append(file)


        file_pairs = list(zip(config_files, slurm_files))
        self._parallel_download_blobs(bucket_name, file_pairs, work_directory, prefix)

        for batch_dir in batch_dirs:
            path = os.path.join(work_directory, batch_dir, "slurm_runner.sh")
            # subprocess.run(["sbatch", path])
            print(f"sbatch {path}")
