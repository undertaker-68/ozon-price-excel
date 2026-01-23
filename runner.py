from fastapi import FastAPI, Request
import subprocess
import os

TOKEN = "SECRET_TOKEN"

app = FastAPI()

@app.post("/run-sync")
async def run_sync(request: Request):
    if request.headers.get("X-Token") != TOKEN:
        return {"ok": False}

    subprocess.Popen(
        ["python3", "/path/to/sync.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    return {"ok": True}
