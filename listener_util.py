from asyncio import Task


class Listener:
    def __init__(self, logger):
        self.listener_task: Task
        self.logger = logger
        # if not self.conn:
        #     raise Exception("DB connection could not be established")

    def _listen(self) -> None:
        raise Exception
