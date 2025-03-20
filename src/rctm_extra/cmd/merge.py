from rctm_extra.cmd.base import BaseCommand


class MergeCommand(BaseCommand):
    def __init__(self, args):
        super().__init__(args)

    def execute(self):
        print("merge command")
