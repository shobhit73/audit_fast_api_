from fastapi import FastAPI
import os
import sys

app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "minimal_test",
        "cwd": os.getcwd(),
        "sys_path": sys.path,
        "file": __file__
    }

@app.get("/test")
async def test():
    return {"status": "ok"}
