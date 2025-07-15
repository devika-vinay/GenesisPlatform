from fastapi import FastAPI

app = FastAPI(title="GÉNESIS API")

@app.get("/health")
def health():
    return {"status": "ok"}
