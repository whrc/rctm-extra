import subprocess
import os

from rctm_extra.cmd.base import BaseCommand


class InitCommand(BaseCommand):
    __RCTM_REPOSITORY = "https://github.com/whrc/RCTM.git"

    def __init__(self, args):
        super().__init__(args)

    def _clone_repository(self, repo_url: str, target_path: str):
        try:
            subprocess.run(f"git clone {repo_url} {target_path}", shell=True, check=True)
            print(f"Successfully cloned RCTM repository to {target_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error cloning repository: {e}")
            raise

    def execute(self):
        rctm_target_dir = os.path.expanduser("~/RCTM")
        if not os.path.exists(rctm_target_dir):
            print("Couldn't find RCTM in the home folder. Cloning it...")
            self._clone_repository(self.__RCTM_REPOSITORY, rctm_target_dir)

        print("\n\nYou're all set!")
