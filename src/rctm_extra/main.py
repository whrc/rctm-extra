import os
import typer

from rctm_extra.cmd.init import InitCommand
from rctm_extra.cmd.split import SplitCommand
from rctm_extra.cmd.run import RunCommand
from rctm_extra.cmd.merge import MergeCommand
from rctm_extra.cmd.submit import SubmitCommand


app = typer.Typer(
    help="A utility tool that automates RCTM-project related operation in the HPC cluster"
)

@app.command("init")
def init():
    args = type("Args", (), {})()
    InitCommand(args).execute()


@app.command("split")
def split(
    config_path: str = typer.Option(
        ..., "--config-path", "-c", help="Path to the configuration file"
    ),
    remote_batch_path: str = typer.Option(
        ..., "--remote-batch-path", "-p", help="Blob path to store split data without the bucket name"
    ),
    rctm_path: str = typer.Option(
        f"{os.getenv('HOME')}/RCTM", "--rctm-path", "-rctm", help="Path to the RCTM model"
    ),
):
    args = type("Args", (), {
        "config_path": config_path,
        "remote_batch_path": remote_batch_path,
        "rctm_path": rctm_path,
        },
    )()
    SplitCommand(args).execute()


@app.command("submit")
def submit(
    bucket_name: str = typer.Option(
        ..., "--bucket-name", "-b", help="Bucket name"
    ),
    remote_batch_path: str = typer.Option(
        ..., "--remote-batch-path", "-p", help="Blob path to access the split data without the bucket name"
    ),
    local_batch_path: str = typer.Option(
        ..., "--local-batch-path", "-l", help="Local path to store remote data"
    ),
):
    args = type("Args", (), {
        "bucket_name": bucket_name,
        "remote_batch_path": remote_batch_path,
        "local_batch_path": local_batch_path,
        },
    )()
    SubmitCommand(args).execute()


@app.command("run")
def run(
    config_path: str = typer.Option(
        ..., "--config-path", "-c", help="Path to the configuration file"
    ),
):
    args = type("Args", (), {
        "config_path": config_path,
    })()
    RunCommand(args).execute()


@app.command("merge")
def merge():
    args = type("Args", (), {})()
    MergeCommand(args).execute()


def main():
    app()
