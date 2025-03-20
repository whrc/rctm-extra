import concurrent.futures
import os
import subprocess

from rctm_extra.cmd.base import BaseCommand
from rctm_extra.gcp import get_storage_client, list_blobs, download_blob
from rctm_extra.utils import get_batch_dirs


class SubmitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__(args)

    def execute(self):
        bucket_name = self.args.bucket_name
        prefix = self.args.remote_batch_path
        work_directory = self.args.local_batch_path

        os.makedirs(work_directory, exist_ok=True)

        client = get_storage_client()
        files = list_blobs(client, bucket_name, prefix)
        batch_dirs = get_batch_dirs(files, prefix)

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
        download_tasks = []
        for config_file, slurm_file in file_pairs:
            config_raw_file = config_file.replace(f"{prefix}/", "")
            config_dest = os.path.join(work_directory, config_raw_file)
            download_tasks.append((bucket_name, config_file, config_dest))

            slurm_raw_file = slurm_file.replace(f"{prefix}/", "")
            slurm_dest = os.path.join(work_directory, slurm_raw_file)
            download_tasks.append((bucket_name, slurm_file, slurm_dest))

        print("Downloading blobs from the bucket. This may take a while...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            def starmap_helper(func, args_tuple):
                return func(*args_tuple)

            futures = [executor.submit(starmap_helper, download_blob, task) for task in download_tasks]
            concurrent.futures.wait(futures)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Download failed with error: {e}")

        for batch_dir in batch_dirs:
            path = os.path.join(work_directory, batch_dir, "slurm_runner.sh")
            # subprocess.run(["sbatch", path])
            print(f"sbatch {path}")
