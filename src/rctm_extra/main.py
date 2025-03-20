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
def split():
    args = type("Args", (), {})()
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
