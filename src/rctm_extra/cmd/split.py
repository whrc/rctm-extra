import os
import yaml
import xarray as xr
import shutil
import rioxarray
import concurrent.futures

from rctm_extra.cmd.base import BaseCommand
from rctm_extra.gcp import get_storage_client, download_blob, upload_directory
from rctm_extra.spatial import get_dimensions_netcdf
from rctm_extra.types import Batch
from rctm_extra.io import create_slurm_file, create_config_file, make_unique_folder
from rctm_extra.config import X_STEP, Y_STEP


class SplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__(args)

    def _split_input_files(self, batch_objs: list, path_to_input: str, path_to_spin_input: str, path_to_params: str) -> None:
        ds = xr.open_dataset(path_to_input)
        for obj in batch_objs:
            subset = ds.isel(
                x=slice(obj.x_range[0], obj.x_range[1]),
                y=slice(obj.y_range[0], obj.y_range[1]),
            )
            os.makedirs(os.path.dirname(obj.input_path), exist_ok=True)
            subset.to_netcdf(obj.input_path)

        ds.close()

        ds = xr.open_dataset(path_to_spin_input)
        for obj in batch_objs:
            subset = ds.isel(
                x=slice(obj.x_range[0], obj.x_range[1]),
                y=slice(obj.y_range[0], obj.y_range[1]),
            )
            os.makedirs(os.path.dirname(obj.spin_input_path), exist_ok=True)
            subset.to_netcdf(obj.spin_input_path)

        ds.close()

        ds = rioxarray.open_rasterio(path_to_params)
        for obj in batch_objs:
            subset = ds.isel(
                x=slice(obj.x_range[0], obj.x_range[1]),
                y=slice(obj.y_range[0], obj.y_range[1]),
            )
            os.makedirs(os.path.dirname(obj.spatial_params_path), exist_ok=True)
            subset.rio.to_raster(obj.spatial_params_path)

        ds.close()

    def execute(self):
        absolute_config_path = os.path.abspath(self.args.config_path)
        if not os.path.exists(absolute_config_path):
            print(f"couldn't find the given config path: {absolute_config_path}")
            return

        absolute_rctm_path = os.path.abspath(self.args.rctm_path)
        if not os.path.exists(absolute_rctm_path):
            print(f"couldn't find the given RCTM folder: {absolute_rctm_path}")
            return 

        with open(absolute_config_path) as file:
            config_data = yaml.safe_load(file)

        bucket_name = config_data.get("bucket_name")
        site_path = config_data.get("gcloud_workflow_base_dir")

        home = os.getenv("HOME")
        local_base_directory = os.path.join(home, "split_data")
        local_base_directory = make_unique_folder(local_base_directory)

        print(f"created batch directory: {local_base_directory}. do not forget that!!!")

        path_to_input = os.path.join(local_base_directory, "RCTM_inputs.nc")
        path_to_spin_input = os.path.join(local_base_directory, "RCTM_spin_inputs.nc")
        path_to_params = os.path.join(local_base_directory, "spatial_params.tif")

        storage_client = get_storage_client()
        download_tasks = [
            (storage_client, bucket_name, f"{site_path}/RCTM_ins/RCTM_inputs.nc", path_to_input),
            (storage_client, bucket_name, f"{site_path}/RCTM_ins/RCTM_spin_inputs.nc", path_to_spin_input),
            (storage_client, bucket_name, f"{site_path}/params/spatial_params.tif", path_to_params)
        ]

        print("downloading the input data in parallel")
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

        X, Y = get_dimensions_netcdf(path_to_input)
        cell_count = X * Y
        print(f"total cell count = {cell_count}")

        print("creating batch objects")
        batch_objs = Batch.create_list(X, Y, local_base_directory)

        print("splitting input files")
        self._split_input_files(batch_objs, path_to_input, path_to_spin_input, path_to_params)

        print("creating config files")
        remote_batch_path = self.args.remote_batch_path
        config_tasks = [(obj, config_data, remote_batch_path, absolute_rctm_path) for obj in batch_objs]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            def starmap_helper(func, args_tuple):
                return func(*args_tuple)

            futures = [executor.submit(starmap_helper, create_config_file, task) for task in config_tasks]
            concurrent.futures.wait(futures)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Creating config files failed with error: {e}")

        print("creating slurm files")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(create_slurm_file, obj) for obj in batch_objs]
            concurrent.futures.wait(futures)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Creating SLURM files failed with error: {e}")

        upload_tasks = []
        for obj in batch_objs:
            upload_tasks.append((storage_client, bucket_name, obj.local_base_directory, obj.local_batch_path, remote_batch_path))

        print("uploading the split data to the bucket")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            def starmap_helper(func, args_tuple):
                return func(*args_tuple)

            futures = [executor.submit(starmap_helper, upload_directory, task) for task in upload_tasks]
            concurrent.futures.wait(futures)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Download failed with error: {e}")
