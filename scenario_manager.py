import json
import os
from pathlib import Path
from datetime import datetime

SCENARIO_DIR = Path("scenarios")

def init_scenarios_dir():
    if not SCENARIO_DIR.exists():
        os.makedirs(SCENARIO_DIR)

def load_active_scenario():
    """
    Loads custom manual weight overrides if the user provides an 'active_scenario.json'.
    Returns a dictionary of weights, or None if the file doesn't exist.
    """
    init_scenarios_dir()
    filepath = SCENARIO_DIR / "active_scenario.json"
    if filepath.exists():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("weights", None)
        except Exception as e:
            print(f"Error reading active_scenario.json: {e}")
    return None

def save_calibration_state(weights_dict, metadata=None):
    """
    Saves the calibrated weights (and any metadata) to a timestamped file.
    """
    init_scenarios_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = SCENARIO_DIR / f"state_calibrated_BRB_{timestamp}.json"
    
    payload = {
        "calibrated_at": timestamp,
        "base_model": "BRB_Gross_Revenue_NNLS",
        "weights": weights_dict,
    }
    if metadata:
        payload["metadata"] = metadata
        
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)
        print(f"  [SCENARIO] Saved calibration state to {filepath}")
        
        # Also auto-update active_scenario if it doesn't exist
        active_path = SCENARIO_DIR / "active_scenario.json"
        with open(active_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving calibration state: {e}")
