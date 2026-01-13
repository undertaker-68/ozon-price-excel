import os
import subprocess
from flask import Flask, request, jsonify

app = Flask(__name__)

PROJECT_DIR = "/root/google_ozon_prices"
RUN_SCRIPT = os.path.join(PROJECT_DIR, "run_sync.sh")
RUN_TOKEN = os.environ.get("RUN_TOKEN", "")

def _load_env():
    # грузим .env в окружение (на случай systemd без EnvironmentFile)
    env_path = os.path.join(PROJECT_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))

_load_env()
RUN_TOKEN = os.environ.get("RUN_TOKEN", RUN_TOKEN)

@app.get("/health")
def health():
    return jsonify(ok=True)

@app.post("/run-sync")
def run_sync():
    token = request.headers.get("X-Token", "")
    if not RUN_TOKEN or token != RUN_TOKEN:
        return jsonify(ok=False, error="unauthorized"), 401

    # запускаем скрипт и ждём завершения
    p = subprocess.run(
        ["bash", RUN_SCRIPT],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=60*30,  # до 30 минут
        env=os.environ.copy()
    )

    return jsonify(
        ok=(p.returncode == 0),
        code=p.returncode,
        stdout=p.stdout[-4000:],  # хвост логов
        stderr=p.stderr[-4000:],
    ), (200 if p.returncode == 0 else 500)

if __name__ == "__main__":
    # слушаем на всех интерфейсах
    app.run(host="0.0.0.0", port=8000)
