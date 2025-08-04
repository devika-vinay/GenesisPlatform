# 📦 Data Processed Folder – Driver Dataset & Mock Trip Logs

This folder contains the **sample driver dataset** and will store the **mock trip logs** for MX, CO, and CR.

---

## 🚖 1. Sample Driver Dataset

**File:** `sample_drivers.csv`  
**Purpose:** Synthetic driver dataset to be used for generating all mock trip logs during the MVP development phase.

**Do NOT** modify or regenerate this dataset — it must remain consistent across all countries.

### 📂 Schema

| Column Name           | Type   | Example Value       | Description |
|-----------------------|--------|--------------------|-------------|
| `driver_id`           | str    | `MX_D001`          | Unique driver identifier (Country prefix + number) |
| `driver_name`         | str    | `Carlos López`     | Driver’s full name |
| `city`                | str    | `cdmx`             | Driver’s base city |
| `country`             | str    | `MX`               | ISO 2-letter country code |
| `vehicle_type`        | str    | `truck` / `van`    | Type of vehicle assigned |
| `capacity`            | str    | `small` / `medium` / `large` | Vehicle load capacity |
| `base_location_lat`   | float  | `19.345678`        | Base latitude for driver |
| `base_location_lon`   | float  | `-99.123456`       | Base longitude for driver |
| `years_experience`    | int    | `5`                | Years of driving experience |
| `avg_acceptance_rate` | float  | `0.92`             | Avg. acceptance rate for trip offers |
| `avg_completion_rate` | float  | `0.95`             | Avg. completion rate for accepted trips |
| `base_fare`           | float  | `150.00`           | Base trip fare in local currency |
| `price_per_km`        | float  | `10.50`            | Per‑km charge in local currency |

---

## 🛣 2. Expected Schema for `mock_trip_logs.csv`

This file will be generated separately for each country:
data/processed/mx_mock_trip_logs.csv
data/processed/co_mock_trip_logs.csv
data/processed/cr_mock_trip_logs.csv


### 📂 Schema

| Column Name       | Type   | Example Value         | Description |
|-------------------|--------|----------------------|-------------|
| `trip_id`         | str    | `MX_T0001`           | Unique trip identifier (Country prefix + number) |
| `driver_id`       | str    | `MX_D001`            | Links to `driver_id` in `sample_drivers.csv` |
| `booking_id`      | str    | `MX_BKG_0001`        | Links to booking request (if applicable) |
| `pickup_lat`      | float  | `19.345678`          | Pickup latitude |
| `pickup_lon`      | float  | `-99.123456`         | Pickup longitude |
| `dropoff_lat`     | float  | `19.456789`          | Drop-off latitude |
| `dropoff_lon`     | float  | `-99.234567`         | Drop-off longitude |
| `distance_km`     | float  | `12.5`               | Trip distance in kilometers |
| `duration_min`    | float  | `25.0`               | Trip duration in minutes |
| `start_time`      | str    | `2025-07-24 08:30`   | Trip start timestamp (local time) |
| `end_time`        | str    | `2025-07-24 08:55`   | Trip end timestamp (local time) |
| `status`          | str    | `completed` / `cancelled` | Trip completion status |
| `vehicle_type`    | str    | `truck` / `van`      | Vehicle type used for trip |
| `capacity`        | str    | `small` / `medium` / `large` | Capacity used for trip |

---

## 📏 Rules for Creation

1. **Driver Linking:**  
   Each generated trip must link to a `driver_id` from `sample_drivers.csv`.

2. **Geographic Alignment:**  
   - Keep city–country consistency.  
   - Example: A driver from `cdmx` (MX) cannot take a `bogota` (CO) trip.

3. **Starting Location:**  
   - Use `base_location_lat` and `base_location_lon` as the starting point for trips.  
   - Repositioning logic can be added if needed.

4. **Vehicle Feasibility:**  
   - Match `vehicle_type` and `capacity` to booking requirements.

5. **Status Distribution:**  
   - Use `completed` for most trips.  
   - Add a small % of `cancelled` trips for realism.

6. **Realistic Timings:**  
   - Derive `duration_min` from `distance_km` using average urban speed (25–40 km/h).

---

_Last Updated: July 2025_

