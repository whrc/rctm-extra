import os
import yaml
from string import Template

from .types import Batch


JOB_TEMPLATE = """#!/bin/sh

#SBATCH --job-name {job_name}
#SBATCH -o {log_path}
#SBATCH -p compute
#SBATCH -N 1

source /data/venv/bin/activate
pip install -r /opt/requirements.txt

rctm_extra run --config-path {config_path}
"""


def create_slurm_file(batch_obj: Batch) -> None:
    values = {
        "job_name": batch_obj.name,
        "log_path": batch_obj.name,
        "config_path": batch_obj.config_path,
    }
    text = JOB_TEMPLATE.format(**values)
    with open(batch_obj.slurm_script_path, "w") as file:
        file.write(text)


def create_config_file(batch_obj: Batch, config_data: dict, gcloud_base_dir: str, path_to_rctm: str) -> None:
    base_path = f"{gcloud_base_dir}/{batch_obj.name}"
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
        # "spatial_spin_fig_path": f"{base_path}/RCTM_output/spinup/figs/spin_fig_grass-tree.jpg",
        "transient_C_stock_hist": f"{base_path}/RCTM_output/transient/C_stock_hist_grass-tree.nc",
        "transient_flux_hist": f"{base_path}/RCTM_output/transient/flux_hist_grass-tree.nc",
        "C_stock_inits_yaml": f"{path_to_rctm}/RCTM/templates/C_stock_inits.yaml",
        # "C_stock_spin_out_path": f"{base_path}/RCTM_output/spinup/RCTM_C_stocks_spin_output_grass-tree.tif",
        # "C_stock_spin_out_path_point": f"{base_path}/RCTM_output/spinup/RCTM_C_stocks_spin_outputs_grass-tree.csv",
        # "gee_key_json": "/home/dteber/res/gee_key.json", # todo: change this later
        "path_to_RCTM_params": f"{path_to_rctm}/RCTM/templates/RCTM_params.yaml",
        "path_to_RCTM_spatial_params": f"{base_path}/params/spatial_params.tif",
        "path_to_geometry_local": f"{path_to_rctm}/examples/geometries/test_poly.geojson",
        # "path_to_spin_covariates_point": f"{base_path}/RCTM_ins/RCTM_spin_inputs.csv",
        # "path_to_spin_covariates_spatial": f"{base_path}/RCTM_ins/RCTM_spin_inputs.nc",
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
    with open(batch_obj.config_path, "w") as file:
        yaml.dump(config_data, file)


def make_unique_folder(full_path):
    """
    Create a folder at full_path. If it exists, append _2, _3, etc. to the last path component.
    Returns the path of the created folder.
    """
    base_dir = os.path.dirname(full_path)
    folder_name = os.path.basename(full_path.rstrip(os.sep))
    folder_path = os.path.join(base_dir, folder_name)
    counter = 2
    while os.path.exists(folder_path):
        folder_path = os.path.join(base_dir, f"{folder_name}_{counter}")
        counter += 1

    os.makedirs(folder_path)
    return folder_path
