# API – Booking & Distance Estimation 

This guide explains how our booking API turns user input into a distance & duration estimate and selects a driver. It’s written for newcomers—no GIS background required.

---

## 1) What this API does

1. You send two addresses (pickup & dropoff) + a country code (`mx|co|cr`).
2. We geocode those addresses into map coordinates (OpenRouteService / ORS).
3. We snap each coordinate to the nearest known stop from our processed data within a strict radius.
4. If we have data for those two stops, we return distance & duration from the distance matrix.  
   If not, we return a haversine (great-circle) estimate.
5. We match a driver and return them along with the estimate.

## 2) Steps to execute API

1. Run the pipeline using docker commands
2. Open Postman
3. Create a new request of type "POST"
4. Paste the following in the url
    - http://localhost:8000/api/booking
5. Select raw > JSON type for body
6. Use the example below to write user input
```json
{
  "country": "mx",
  "pickup_address": "SN Calle Genaro V. Vásquez, Oaxaca City, OAX, Mexico",
  "dropoff_address": "1099 Otro Ninguno, Hacienda Blanca, OAX, Mexico",
  "vehicle_class": "medium"
}
```
7. Hit send
8. Expected response (example)
```json
{
    "pickup": {
        "address": "SN Calle Genaro V. Vásquez, Oaxaca City, OAX, Mexico",
        "lat": 17.025593,
        "lon": -96.819523
    },
    "dropoff": {
        "address": "1099 Otro Ninguno, Hacienda Blanca, OAX, Mexico",
        "lat": 17.025593,
        "lon": -96.819523
    },
    "used_stops": {
        "pickup_stop_id": null,
        "dropoff_stop_id": null
    },
    "trip_estimate": {
        "distance_km": 0.0,
        "duration_min": null,
        "source": "haversine"
    },
    "matched_driver": {
        "driver_id": "MX_D016",
        "country": "MX",
        "city": "oaxaca",
        "base_location_lat": 17.058965,
        "base_location_lon": -96.772835,
        "capacity": "large",
        "avg_acceptance_rate": 0.8,
        "avg_completion_rate": 0.96
    }
}
```
