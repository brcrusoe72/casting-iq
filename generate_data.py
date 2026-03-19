#!/usr/bin/env python3
"""
Generate realistic investment casting manufacturing data.
Models a Howmet-style aerospace casting shop with:
- 4 vacuum pour furnaces (the constraint)
- Shell building line (6 stations)
- Dewax autoclave
- Finishing/grinding cells
- NDT inspection (X-ray, FPI)

Data characteristics modeled from industry research:
- Typical aerospace casting first-time yield: 70-85%
- OEE on pour furnaces: 55-75% (industry average ~65%)
- Scrap distribution: inclusions 25%, porosity 20%, shell cracks 18%, 
  dimensional 15%, stray grain/freckle 12%, misrun 5%, other 5%
- Pour temperature: 1480-1560°C for nickel superalloys
- Mold preheat: 1000-1100°C
- Shell room humidity: 40-60% RH (critical for shell integrity)

HIDDEN PATTERNS (for the demo to discover):
1. Shell room humidity >55% correlates with 2.3x shell crack rate
2. Pour sequence position 1 (cold furnace) has 1.8x scrap vs positions 3-6
3. Night shift has 15% higher dimensional defect rate (fatigue/lighting)
4. Short stop clusters (3+ in 30min) precede furnace trips 87% of the time
5. Alloy CMSX-4 (single crystal) has dramatically different yield than IN718
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os

np.random.seed(42)

# --- Configuration ---
START_DATE = datetime(2025, 10, 1)
END_DATE = datetime(2026, 3, 15)
NUM_DAYS = (END_DATE - START_DATE).days

FURNACES = ["VIM-01", "VIM-02", "VIM-03", "VIM-04"]
ALLOYS = {
    "IN718": {"pour_temp": (1500, 12), "preheat": (1050, 15), "base_yield": 0.82, "weight": 0.35},
    "IN738": {"pour_temp": (1520, 10), "preheat": (1060, 12), "base_yield": 0.79, "weight": 0.25},
    "Waspaloy": {"pour_temp": (1490, 15), "preheat": (1040, 18), "base_yield": 0.84, "weight": 0.15},
    "CMSX-4": {"pour_temp": (1545, 8), "preheat": (1080, 10), "base_yield": 0.62, "weight": 0.15},  # Single crystal - much harder
    "René-80": {"pour_temp": (1510, 11), "preheat": (1055, 14), "base_yield": 0.80, "weight": 0.10},
}
SHIFTS = ["Day", "Swing", "Night"]
PART_TYPES = ["HPT Blade", "LPT Blade", "Nozzle Guide Vane", "Shroud", "Seal Ring", "Combustor Liner"]
DEFECT_TYPES = ["Inclusion", "Porosity", "Shell Crack", "Dimensional", "Stray Grain", "Misrun", "Hot Tear", "Surface Finish"]


def generate_production_events():
    """Generate pour-level production events with realistic patterns."""
    events = []
    event_id = 10000

    for day_offset in range(NUM_DAYS):
        date = START_DATE + timedelta(days=day_offset)
        weekday = date.weekday()

        # Skip some Sundays (reduced production)
        if weekday == 6 and np.random.random() < 0.3:
            continue

        # Shell room humidity varies by season and day
        base_humidity = 48 + 8 * np.sin(2 * np.pi * day_offset / 365)  # Seasonal
        daily_humidity = base_humidity + np.random.normal(0, 4)
        daily_humidity = np.clip(daily_humidity, 35, 68)

        for shift_idx, shift in enumerate(SHIFTS):
            # Night shift: fewer pours, slightly worse quality
            pours_per_shift = np.random.poisson(8 if shift != "Night" else 6)

            for pour_num in range(pours_per_shift):
                event_id += 1
                furnace = np.random.choice(FURNACES)

                # Pick alloy (weighted)
                alloy_names = list(ALLOYS.keys())
                alloy_weights = [ALLOYS[a]["weight"] for a in alloy_names]
                alloy = np.random.choice(alloy_names, p=alloy_weights)
                alloy_params = ALLOYS[alloy]

                # Pour sequence position (1-8 per campaign)
                pour_position = (pour_num % 8) + 1

                # Process parameters
                pour_temp = np.random.normal(alloy_params["pour_temp"][0], alloy_params["pour_temp"][1])
                preheat_temp = np.random.normal(alloy_params["preheat"][0], alloy_params["preheat"][1])
                humidity = daily_humidity + np.random.normal(0, 2)

                # Parts per pour (tree of blades)
                parts_per_pour = np.random.choice([4, 6, 8, 12], p=[0.15, 0.35, 0.35, 0.15])

                part_type = np.random.choice(PART_TYPES, p=[0.30, 0.25, 0.20, 0.10, 0.10, 0.05])

                # --- YIELD CALCULATION WITH HIDDEN PATTERNS ---
                base_yield = alloy_params["base_yield"]

                # Pattern 1: Humidity > 55% → shell cracks increase
                humidity_penalty = 0
                if humidity > 55:
                    humidity_penalty = -0.08 * (humidity - 55) / 10
                elif humidity < 40:
                    humidity_penalty = -0.03  # Too dry also bad

                # Pattern 2: Pour position 1 (cold furnace) → higher scrap
                position_penalty = -0.06 if pour_position == 1 else (-0.02 if pour_position == 2 else 0)

                # Pattern 3: Night shift → more dimensional defects
                shift_penalty = -0.04 if shift == "Night" else 0

                # Pattern 4: Temperature deviation → quality impact
                temp_dev = abs(pour_temp - alloy_params["pour_temp"][0])
                temp_penalty = -0.02 * (temp_dev / alloy_params["pour_temp"][1]) if temp_dev > alloy_params["pour_temp"][1] else 0

                # Combined yield
                effective_yield = base_yield + humidity_penalty + position_penalty + shift_penalty + temp_penalty
                effective_yield = np.clip(effective_yield, 0.30, 0.98)

                # Determine good vs scrap parts
                good_parts = 0
                scrap_parts = 0
                defects = []

                for _ in range(parts_per_pour):
                    if np.random.random() < effective_yield:
                        good_parts += 1
                    else:
                        scrap_parts += 1
                        # Determine defect type based on conditions
                        defect_probs = {
                            "Inclusion": 0.25,
                            "Porosity": 0.20,
                            "Shell Crack": 0.18 + (0.15 if humidity > 55 else 0),  # Humidity effect
                            "Dimensional": 0.15 + (0.08 if shift == "Night" else 0),  # Night shift
                            "Stray Grain": 0.12 if alloy == "CMSX-4" else 0.05,  # Single crystal
                            "Misrun": 0.05 + (0.05 if pour_temp < alloy_params["pour_temp"][0] - 15 else 0),
                            "Hot Tear": 0.04,
                            "Surface Finish": 0.03,
                        }
                        total = sum(defect_probs.values())
                        defect_probs = {k: v/total for k, v in defect_probs.items()}
                        defect = np.random.choice(list(defect_probs.keys()), p=list(defect_probs.values()))
                        defects.append(defect)

                # Timing
                pour_start = date.replace(
                    hour=[6, 14, 22][shift_idx],
                    minute=0
                ) + timedelta(minutes=int(pour_num * 45 + np.random.normal(0, 10)))

                pour_duration_min = np.random.normal(35, 5)  # Minutes per pour

                events.append({
                    "event_id": f"EVT-{event_id}",
                    "date": date.strftime("%Y-%m-%d"),
                    "timestamp": pour_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "shift": shift,
                    "furnace": furnace,
                    "alloy": alloy,
                    "part_type": part_type,
                    "pour_position": pour_position,
                    "pour_temp_c": round(pour_temp, 1),
                    "mold_preheat_c": round(preheat_temp, 1),
                    "shell_humidity_rh": round(humidity, 1),
                    "parts_cast": parts_per_pour,
                    "parts_good": good_parts,
                    "parts_scrap": scrap_parts,
                    "first_time_yield": round(good_parts / parts_per_pour, 3) if parts_per_pour > 0 else 0,
                    "defects": json.dumps(defects),
                    "pour_duration_min": round(pour_duration_min, 1),
                    "effective_yield_target": round(effective_yield, 3),
                })

    return pd.DataFrame(events)


def generate_downtime_events(production_df):
    """Generate equipment downtime events with short-stop clustering pattern."""
    downtime = []
    dt_id = 50000

    for day_offset in range(NUM_DAYS):
        date = START_DATE + timedelta(days=day_offset)
        if date.weekday() == 6 and np.random.random() < 0.3:
            continue

        for shift_idx, shift in enumerate(SHIFTS):
            shift_start = date.replace(hour=[6, 14, 22][shift_idx])

            for furnace in FURNACES:
                # Short stops (< 15 min)
                num_short_stops = np.random.poisson(2.5)
                short_stop_times = []

                for _ in range(num_short_stops):
                    dt_id += 1
                    stop_time = shift_start + timedelta(minutes=np.random.uniform(10, 470))
                    duration = np.random.exponential(5) + 1  # 1-30 min, typically 3-8
                    duration = min(duration, 14)

                    reason = np.random.choice(
                        ["Thermocouple check", "Shell alignment", "Vacuum leak minor",
                         "Operator break", "Material staging", "Mold positioning",
                         "Pour rate adjustment", "Chamber pressure fluctuation"],
                        p=[0.20, 0.15, 0.15, 0.15, 0.12, 0.10, 0.08, 0.05]
                    )

                    short_stop_times.append(stop_time)
                    downtime.append({
                        "event_id": f"DT-{dt_id}",
                        "date": date.strftime("%Y-%m-%d"),
                        "timestamp": stop_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "shift": shift,
                        "furnace": furnace,
                        "duration_min": round(duration, 1),
                        "category": "Short Stop",
                        "reason": reason,
                        "is_planned": False,
                    })

                # Pattern 4: If 3+ short stops within 30 minutes, 87% chance of a major breakdown
                short_stop_times.sort()
                cluster_detected = False
                for i in range(len(short_stop_times) - 2):
                    if (short_stop_times[i+2] - short_stop_times[i]).total_seconds() < 1800:
                        cluster_detected = True
                        break

                if cluster_detected and np.random.random() < 0.87:
                    dt_id += 1
                    breakdown_time = short_stop_times[-1] + timedelta(minutes=np.random.uniform(15, 60))
                    breakdown_duration = np.random.exponential(120) + 30  # 30-300 min

                    breakdown_reason = np.random.choice(
                        ["Vacuum pump failure", "Heating element burnout", "Crucible crack",
                         "Power supply trip", "Cooling water loss", "Induction coil fault"],
                        p=[0.25, 0.25, 0.15, 0.15, 0.10, 0.10]
                    )

                    downtime.append({
                        "event_id": f"DT-{dt_id}",
                        "date": date.strftime("%Y-%m-%d"),
                        "timestamp": breakdown_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "shift": shift,
                        "furnace": furnace,
                        "duration_min": round(breakdown_duration, 1),
                        "category": "Breakdown",
                        "reason": breakdown_reason,
                        "is_planned": False,
                    })

                # Planned maintenance (weekly-ish per furnace)
                if np.random.random() < 0.08:
                    dt_id += 1
                    pm_time = shift_start + timedelta(hours=1)
                    downtime.append({
                        "event_id": f"DT-{dt_id}",
                        "date": date.strftime("%Y-%m-%d"),
                        "timestamp": pm_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "shift": shift,
                        "furnace": furnace,
                        "duration_min": round(np.random.normal(180, 30), 1),
                        "category": "Planned Maintenance",
                        "reason": np.random.choice(["PM - Furnace rebuild", "PM - Thermocouple replacement",
                                                     "PM - Vacuum system service", "PM - Crucible change"]),
                        "is_planned": True,
                    })

    return pd.DataFrame(downtime)


def generate_cycle_time_data():
    """Generate cycle time data showing time spent at each operation."""
    operations = [
        ("Wax Injection", 0.5, 0.1),
        ("Wax Assembly", 1.0, 0.2),
        ("Shell Building", 48.0, 8.0),   # The long one — 5-7 dipping cycles
        ("Dewax", 2.0, 0.3),
        ("Mold Preheat", 4.0, 0.5),
        ("Pour", 0.6, 0.1),
        ("Cooling", 8.0, 1.0),
        ("Knockout", 1.5, 0.3),
        ("Cut-off", 1.0, 0.2),
        ("Grinding", 3.0, 0.5),
        ("Heat Treat/HIP", 12.0, 2.0),
        ("Machining", 4.0, 1.0),
        ("FPI Inspection", 2.0, 0.3),
        ("X-Ray", 3.0, 0.5),
        ("Dimensional", 2.0, 0.4),
        ("Final Inspection", 1.5, 0.2),
    ]

    # Wait times between operations (the hidden waste)
    records = []
    for op_name, process_hrs, std_hrs in operations:
        for _ in range(200):  # Sample of 200 parts
            process_time = max(0.1, np.random.normal(process_hrs, std_hrs))
            queue_time = np.random.exponential(process_hrs * 0.4)  # Queue ~ 40% of process on average
            records.append({
                "operation": op_name,
                "process_time_hrs": round(process_time, 2),
                "queue_time_hrs": round(queue_time, 2),
                "total_time_hrs": round(process_time + queue_time, 2),
            })

    return pd.DataFrame(records)


if __name__ == "__main__":
    print("Generating production events...")
    prod = generate_production_events()
    prod.to_csv("casting-iq/data/production_events.csv", index=False)
    print(f"  {len(prod)} production events")

    print("Generating downtime events...")
    dt = generate_downtime_events(prod)
    dt.to_csv("casting-iq/data/downtime_events.csv", index=False)
    print(f"  {len(dt)} downtime events")

    print("Generating cycle time data...")
    ct = generate_cycle_time_data()
    ct.to_csv("casting-iq/data/cycle_times.csv", index=False)
    print(f"  {len(ct)} cycle time records")

    # Summary stats
    print(f"\nDate range: {prod['date'].min()} to {prod['date'].max()}")
    print(f"Total parts cast: {prod['parts_cast'].sum()}")
    print(f"Total good: {prod['parts_good'].sum()}")
    print(f"Total scrap: {prod['parts_scrap'].sum()}")
    print(f"Overall FTY: {prod['parts_good'].sum() / prod['parts_cast'].sum():.1%}")
    print(f"Breakdowns: {len(dt[dt['category'] == 'Breakdown'])}")
