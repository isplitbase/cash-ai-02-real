from fastapi import FastAPI, Body
from typing import Any
from app.pipeline.runner101 import run_colab101

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/v1/pipeline")
def pipeline(payload: Any = Body(...)):
    # JSON body をそのまま受け取る（dictでもlistでもOK）
    return run_colab101(payload)

