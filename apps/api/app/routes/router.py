from fastapi import FastAPI
from .booking_api import router as booking_router

app = FastAPI(title="Genesis Pilot API", version="0.1")

# health endpoint (handy for compose healthchecks)
@app.get("/health")
def health():
    return {"ok": True}

# mount your booking API routes
app.include_router(booking_router)
