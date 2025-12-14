from asyncio import Task


class Listener:
    def __init__(self, logger):
        self.listener_task: Task
        self.logger = logger

    def _listen(self) -> None:
        while True:
            self.logger.warning("Listening for events...")
        raise Exception("DB connection could not be established")
