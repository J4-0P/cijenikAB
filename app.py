from fastapi import FastAPI # type: ignore
from pydantic import BaseModel

app = FastAPI()

# Root endpoint
@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI"}