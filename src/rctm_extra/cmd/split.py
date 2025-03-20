from rctm_extra.cmd.base import BaseCommand


class SplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__(args)

    def execute(self):
        print("hello from the split command")
