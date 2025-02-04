
from fastapi import FastAPI
from pydantic import BaseModel

from ray import serve

app = FastAPI()


class InputInterface(BaseModel):
    user_input: str


class OutputInterface(BaseModel):
    message: str


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0.1})
@serve.ingress(app)
class DemoApplication:
    def __init__(self):
        self.message = "This is a Demo Application!"

        self.logger.info("App is initialized!")
        self.logger.info("Testing logger: This is an INFO log!")
        self.logger.warning("Testing logger: This is an WARN log!")
        self.logger.debug("Testing logger: This is an DEBUG log!")
        self.logger.error("Testing logger: This is an ERROR log!")

    def run(self, input: InputInterface) -> OutputInterface:
        user_message = input.user_input
        processed_user_message = self.preprocess_user_message(user_message)
        return OutputInterface(message=processed_user_message)

    def preprocess_user_message(self, user_message: str) -> str:
        return f"{user_message} {self.message}"


app = DemoApplication.bind()
