import random
from string import ascii_letters, digits
import os
import yaml
import xarray as xr
import shutil
import rioxarray
import concurrent.futures
from dataclasses import dataclass
from pathlib import Path
from string import Template

from google.cloud import storage
from rctm_extra.cmd.base import BaseCommand


X_STEP = 10
Y_STEP = 10

JOB_TEMPLATE = """
#!/bin/bash

#SBATCH --job-name {job_name}
#SBATCH -o {log_path}
#SBATCH -p compute
#SBATCH -N 1

source /data/venv/bin/activate
pip install -r /opt/requirements.txt

# run the model
"""

@dataclass
class Batch:
    name: str
    x_range: tuple
    y_range: tuple
    local_base_directory: str
    local_batch_path: str
    input_path: str
    spin_input_path: str
    spatial_params_path: str
    config_path: str
    slurm_script_path: str


class SplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__(args)

    def _generate_hidden_folder(self, base_dir="."):
        folder_name = "." + "".join(random.choices(ascii_letters + digits, k=10))
        folder_path = os.path.join(base_dir, folder_name)

        os.makedirs(folder_path, exist_ok=True)
        return folder_path

    @staticmethod
    def _download_blob(download_info):
        """Download a blob from a bucket to a local file.
        
        Args:
            download_info: A tuple of (bucket_name, source_blob_name, destination_file_name)
        """
        bucket_name, source_blob_name, destination_file_name = download_info
        storage_client = storage.Client(project="rangelands-explo-1571664594580")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        print(
            "Downloaded storage object {} from bucket {} to local file {}.".format(
                source_blob_name, bucket_name, destination_file_name
            )
        )
        return destination_file_name

    def _parallel_download_blobs(self, tasks: list):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(self._download_blob, task) for task in tasks]
            concurrent.futures.wait(futures)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Download failed with error: {e}")

    def _create_batch_objects(self, x_dim: int, y_dim: int, local_base_dir: str) -> list:
        batch_objs = []
        for x in range(0, x_dim, X_STEP):
            for y in range(0, y_dim, Y_STEP):
                x_start = x
                x_end = min(x + X_STEP, x_dim)
                y_start = y
                y_end = min(y + Y_STEP, y_dim)
                batch_name = f"batch_x_{x_start}-{x_end}_y_{y_start}-{y_end}"
                obj = Batch(
                    name=batch_name,
                    x_range=(x_start, x_end),
                    y_range=(y_start, y_end),
                    local_base_directory=local_base_dir,
                    local_batch_path=os.path.join(
                        local_base_dir, batch_name
                    ),
                    input_path=os.path.join(
                        local_base_dir, batch_name, "RCTM_ins", "RCTM_inputs.nc"
                    ),
                    spin_input_path=os.path.join(
                        local_base_dir, batch_name, "RCTM_ins", "RCTM_spin_inputs.nc"
                    ),
                    spatial_params_path=os.path.join(
                        local_base_dir, batch_name, "params", "spatial_params.tif"
                    ),
                    config_path=os.path.join(
                        local_base_dir, batch_name, "config.yaml"
                    ),
                    slurm_script_path=os.path.join(
                        local_base_dir, batch_name, "slurm_runner.sh"
                    )
                )
                batch_objs.append(obj)

        return batch_objs

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

    def _create_config_files(self, batch_objs: list, config_data: dict, gcloud_base_dir: str, path_to_rctm: str) -> None:
        for obj in batch_objs:
            base_path = f"{gcloud_base_dir}/{obj.name}"
            paths = {
                "gcloud_workflow_base_dir": base_path,
                "landsat_save_dir": f"{base_path}/landsat",
                "modis_save_dir": f"{base_path}/modis",
                "covariates_save_dir": f"{base_path}/covariates",
                "landcover_save_dir": f"{base_path}/landcover",
                "modis_smooth_in_dir": f"{base_path}/modis",
                "modis_smooth_out_dir": f"{base_path}/modis_smooth",
                "starfm_in_modis_dir": f"{base_path}/modis_smooth",
                "starfm_in_landsat_dir": f"{base_path}/landsat",
                "starfm_out_dir": f"{base_path}/starfm",
                "RCTM_input_dir": f"{base_path}/RCTM_ins",
                "spatial_param_outname": f"{base_path}/params/spatial_params.tif",
                "fused_landcover_outname": f"{base_path}/landcover/fused_landcover.tif",
                "spatial_spin_fig_path": f"{base_path}/RCTM_output/spinup/figs/spin_fig_grass-tree.jpg",
                "transient_C_stock_hist": f"{base_path}/RCTM_output/transient/C_stock_hist_grass-tree.nc",
                "transient_flux_hist": f"{base_path}/RCTM_output/transient/flux_hist_grass-tree.nc",
                "C_stock_inits_yaml": f"{path_to_rctm}/RCTM/templates/C_stock_inits.yaml",
                "C_stock_spin_out_path": f"{base_path}/RCTM_output/spinup/RCTM_C_stocks_spin_output_grass-tree.tif",
                "C_stock_spin_out_path_point": f"{base_path}/RCTM_output/spinup/RCTM_C_stocks_spin_outputs_grass-tree.csv",
                # "gee_key_json": "/home/dteber/res/gee_key.json", # todo: change this later
                "path_to_RCTM_params": f"{path_to_rctm}/RCTM/templates/RCTM_params.yaml",
                "path_to_RCTM_spatial_params": f"{base_path}/params/spatial_params.tif",
                "path_to_geometry_local": f"{path_to_rctm}/examples/geometries/test_poly.geojson",
                "path_to_spin_covariates_point": f"{base_path}/RCTM_ins/RCTM_spin_inputs.csv",
                "path_to_spin_covariates_spatial": f"{base_path}/RCTM_ins/RCTM_spin_inputs.nc",
                "starfm_config": f"{path_to_rctm}/RCTM/config/input_ref.txt",
                "starfm_source": f"{path_to_rctm}/RCTM/remote_sensing/starfm_source/",
                "transient_covariate_path": f"{base_path}/RCTM_ins/RCTM_inputs.nc",
                "workflows_path": f"{path_to_rctm}/examples/workflows/test_single_poly/",
            }

            ignore = ["ee_geometry_save_dir", "path_to_existing_ee_geom", "geometry_polygon", "shape_name_col"]
            for elem in ignore:
                config_data.pop(elem, None)
            for key, value in paths.items():
                config_data[key] = value
            with open(obj.config_path, "w") as file:
                yaml.dump(config_data, file)

    def _create_slurm_files(self, batch_objs: list) -> None:
        for obj in batch_objs:
            values = {
                "job_name": obj.name,
                "log_path": "CHANGE_ME",
            }

            template = Template(JOB_TEMPLATE)
            text = template.safe_substitute(values)
            with open(obj.slurm_script_path, "w") as file:
                file.write(text)

    @staticmethod
    def _upload_directory(args):
        bucket_name, batch_obj, source_directory, destination_directory = args

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        local_batch_dir = Path(source_directory)
        for local_file in local_batch_dir.rglob("*"):
            if local_file.is_file():
                relative_path = local_file.relative_to(batch_obj.local_base_directory)
                blob_path = f"{destination_directory}/{relative_path}"
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(local_file))


    # upload_directory(bucket_name, obj.local_batch_path, base_batch_folder)
    def _parallel_upload_directory(self, bucket_name, batch_objs, base_batch_folder, max_workers=4):
        tasks = []
        for obj in batch_objs:
            tasks.append((bucket_name, obj, obj.local_batch_path, base_batch_folder))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self._upload_directory, tasks)

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

        local_base_directory = self._generate_hidden_folder()
        print(f"created temp directory: {local_base_directory}")
        os.makedirs(local_base_directory, exist_ok=True)

        path_to_input = os.path.join(local_base_directory, "RCTM_inputs.nc")
        path_to_spin_input = os.path.join(local_base_directory, "RCTM_spin_inputs.nc")
        path_to_params = os.path.join(local_base_directory, "spatial_params.tif")

        download_tasks = [
            (bucket_name, f"{site_path}/RCTM_ins/RCTM_inputs.nc", path_to_input),
            (bucket_name, f"{site_path}/RCTM_ins/RCTM_spin_inputs.nc", path_to_spin_input),
            (bucket_name, f"{site_path}/params/spatial_params.tif", path_to_params)
        ]

        print("downloading the input data in parallel")
        self._parallel_download_blobs(download_tasks)

        with xr.open_dataset(path_to_input) as ds:
            X = ds.sizes["x"]
            Y = ds.sizes["y"]

        cell_count = X * Y
        print(f"total cell count = {cell_count}")

        print("creating batch objects")
        batch_objs = self._create_batch_objects(X, Y, local_base_directory)

        print("splitting input files")
        self._split_input_files(batch_objs, path_to_input, path_to_spin_input, path_to_params)

        print("creating config and slurm files")
        remote_batch_path = self.args.remote_batch_path
        self._create_config_files(batch_objs, config_data, remote_batch_path, absolute_rctm_path)
        self._create_slurm_files(batch_objs)

        print("uploading the split data to the bucket")
        self._parallel_upload_directory(bucket_name, batch_objs[:5], remote_batch_path)

        print("removing the temporary folder")
        shutil.rmtree(local_base_directory)
