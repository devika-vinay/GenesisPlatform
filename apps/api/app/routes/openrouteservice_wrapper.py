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
                    data = r.json()
                    summary, segments = {}, []
                    if isinstance(data, dict) and data.get("routes"):
                        route = data["routes"][0] or {}
                        summary  = route.get("summary")  or {}
                        segments = route.get("segments") or []
                    elif isinstance(data, dict) and data.get("features"):
                        props    = (data["features"][0] or {}).get("properties") or {}
                        summary  = props.get("summary")  or {}
                        segments = props.get("segments") or []

                    dist = summary.get("distance")
                    dur  = summary.get("duration")

                    # Fallback: sum segment distances/durations if summary is incomplete
                    if segments:
                        if dist is None:
                            dist = sum((s or {}).get("distance", 0.0) for s in segments)
                        if dur is None:
                            dur = sum((s or {}).get("duration", 0.0) for s in segments)

                    # If still missing, *don’t* raise—signal the caller to leave blanks
                    if dist is None or dur is None:
                        return {"error": "missing distance/duration in ORS payload"}

                    return {"distance_m": float(dist), "duration_s": float(dur)}

                # Retry on 429 or transient 5xx
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    if attempt < self.retries - 1:
                        time.sleep(2 * (attempt + 1))  # 2s, 4s, …
                        continue

                # Non-retryable or retries exhausted
                return {"error": f"{r.status_code}: {r.text}"}

            except requests.RequestException as exc:
                if attempt < self.retries - 1:
                    time.sleep(2)
                    continue
                return {"error": f"Request failed: {exc}"}

