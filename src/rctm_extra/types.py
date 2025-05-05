import os
from dataclasses import dataclass

from rctm_extra.config import X_STEP, Y_STEP


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
