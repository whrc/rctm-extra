import os
import typer

from rctm_extra.cmd.init import InitCommand
from rctm_extra.cmd.split import SplitCommand
from rctm_extra.cmd.run import RunCommand
from rctm_extra.cmd.merge import MergeCommand


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
        ..., "--remote-batch-path", "-p", help="Blob path to store split data"
    ),
    rctm_path: str = typer.Option(
        f"{os.getenv('HOME')}/RCTM", "--rctm-path", "-rctm", help="Path to the RCTM model"
    )
):
    args = type("Args", (), {
        "config_path": config_path,
        "remote_batch_path": remote_batch_path,
        "rctm_path": rctm_path,
        },
    )()
    SplitCommand(args).execute()


@app.command("run")
def run():
    args = type("Args", (), {})()
    RunCommand(args).execute()


@app.command("merge")
def merge():
    args = type("Args", (), {})()
    MergeCommand(args).execute()


def main():
    app()
