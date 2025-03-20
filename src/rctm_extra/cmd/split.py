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
from rctm_extra.file import generate_hidden_folder
from rctm_extra.gcp import get_storage_client, download_blob, upload_directory
from rctm_extra.spatial import get_dimensions_netcdf


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

# install rctm_extra

rctm_extra --config-path {config_path}
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

    @staticmethod
    def create_list(x_dim: int, y_dim: int, local_base_dir: str) -> list:
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

        local_base_directory = generate_hidden_folder()
        print(f"created temp directory: {local_base_directory}")
        os.makedirs(local_base_directory, exist_ok=True)

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

        print("creating config and slurm files")
        remote_batch_path = self.args.remote_batch_path
        self._create_config_files(batch_objs, config_data, remote_batch_path, absolute_rctm_path)
        self._create_slurm_files(batch_objs)

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

        print("removing the temporary folder")
        shutil.rmtree(local_base_directory)
