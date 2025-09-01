import asyncio
import logging
import threading
from fastapi import FastAPI
from pydantic import BaseModel

import ray
from ray import serve
from ray.serve.schema import LoggingConfig

app = FastAPI()


class InputInterface(BaseModel):
    user_input: str


class OutputInterface(BaseModel):
    message: str


class BackgroundTasks(threading.Thread):
    def __init__(self, current_active_actor_id: str):
        super().__init__()
        self.is_stopped = False
        self.current_active_actor_id = current_active_actor_id
        self.failed = True

    def run(self, *args, **kwargs):
        raise Exception

    def stop(self):
        self.is_stopped = True


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0.1}, logging_config=LoggingConfig(encoding="JSON"))
@serve.ingress(app)
class DemoApplication:
    def __init__(self):
        self.message = "This is a Demo Application!"

        self.logger = logging.getLogger("ray.serve")

        self.logger.info("App is initialized!")
        self.logger.info("Testing logger: This is an INFO log!")
        self.logger.warning("Testing logger: This is an WARN log!")
        self.logger.debug("Testing logger: This is an DEBUG log!")
        self.logger.error("Testing logger: This is an ERROR log!")
        
        self.current_active_actor_id = ray.get_runtime_context().get_actor_id()
        self.start_background_task()

    @app.post("/run")
    def run(self, input: InputInterface) -> OutputInterface:
        user_message = input.user_input
        processed_user_message = self.preprocess_user_message(user_message)
        return OutputInterface(message=processed_user_message)

    def preprocess_user_message(self, user_message: str) -> str:
        return f"{user_message} {self.message}"

    def start_background_task(self):
        self.background_task = BackgroundTasks(
            current_active_actor_id=self.current_active_actor_id
        )
        self.background_task.start()
        threading.excepthook = self.restart_thread

    def stop_background_task(self):
        if hasattr(self, "background_task"):
            self.background_task.stop()
            del self.background_task

    def restart_thread(self, args):
        self.background_task = BackgroundTasks(current_active_actor_id=self.current_active_actor_id)
        self.background_task.start()
        self.logger.warning("THREAD FAILED! RESTARTING.")

    # # Ray internal health check
    # async def check_health(self):
    #     await asyncio.sleep(10)

    #     if not self.background_task.failed:
    #         raise Exception

    def __del__(self):
        self.stop_background_task()


app = DemoApplication.bind()
