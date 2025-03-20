import xarray as xr


def get_dimensions_netcdf(file_path: str, x_dim: str = "x", y_dim: str = "y"):
    with xr.open_dataset(file_path) as ds:
        X = ds.sizes[x_dim]
        Y = ds.sizes[y_dim]

    return X, Y
