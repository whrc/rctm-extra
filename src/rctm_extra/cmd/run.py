import os
import sys

from rctm_extra.cmd.base import BaseCommand


class RunCommand(BaseCommand):
    def __init__(self, args):
        super().__init__(args)

    def execute(self):
        home = os.getenv("HOME")
        rctm_path = os.path.join(home, "RCTM")

        sys.path.insert(0, rctm_path)
        os.environ["PYTHONPATH"] = ""
        os.environ["RCTMPATH"] = os.path.join(rctm_path, "RCTM")

        from RCTM.pipelines.RCTM_model_pipeline import RCTMPipeline

        config_path = self.args.config_path
        if not os.path.exists(config_path) or not config_path.endswith(".yaml"):
            print("The given path not found or not a config file. Aborting")
            sys.exit(1)

        pipeline = RCTMPipeline(config_filename=config_path)
        pipeline.run_RCTM()
