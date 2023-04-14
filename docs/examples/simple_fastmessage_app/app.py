from messageflux import FastMessage

from pydantic import BaseModel

app = FastMessage(default_output_device="null_output_device")

class InputModel(BaseModel):
    a: str
    b: int

class SecondInputModel(BaseModel):
    e: str
    f: int

class OutputModel(BaseModel):
    c: str
    d: int

# @app.register_callback should be private
# app.handle_message should be private

# suggestion: @app.route
@app.map("service_input_topic", "service_output_topic")
def handle_message(message: InputModel) -> OutputModel:
    return OutputModel(a=message.a, b=message.b)


@app.map("third_input_topic")
def handle_third_message(a: str, b: int) -> OutputModel:
    return OutputModel(c=a, d=b)