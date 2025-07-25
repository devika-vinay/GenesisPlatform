from __future__ import annotations
import os, requests, time
from dotenv import load_dotenv 


class OpenRouteServiceWrapper:

    load_dotenv()  # loads .env when running locally

    def __init__(
        self,
        api_key: str | None = None,
        profile: str = "driving-hgv",   # heavy‑goods; good default for trucks
        retries: int = 3,
        timeout: int = 10,
    ):
        self.api_key = api_key or os.getenv("ORS_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "ORS_API_KEY missing – pass via env or constructor argument"
            )
        self.profile = profile
        self.retries = retries
        self.timeout = timeout
        self.base_url = f"https://api.openrouteservice.org/v2/directions/{self.profile}"

    def get_route(
        self, start: tuple[float, float], end: tuple[float, float]
    ) -> dict[str, float] | dict[str, str]:
        """
        Parameters
        ----------
        start, end : (lon, lat)

        Returns
        -------
        {'distance_m': float, 'duration_s': float}
        or {'error': str}
        """
        headers = {"Authorization": self.api_key, "Content-Type": "application/json"}
        body = {"coordinates": [start, end]}

        for attempt in range(self.retries):
            try:
                r = requests.post(self.base_url, headers=headers, json=body, timeout=self.timeout)
                if r.status_code == 200:
                    summary = r.json()["routes"][0]["summary"]
                    return {"distance_m": summary["distance"], "duration_s": summary["duration"]}
                else:
                    return {"error": f"{r.status_code}: {r.text}"}
            except requests.RequestException as exc:
                if attempt < self.retries - 1:
                    time.sleep(2)
                else:
                    return {"error": f"Request failed: {exc}"}
