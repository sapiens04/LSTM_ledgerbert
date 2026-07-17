import joblib
import json
import numpy as np

for name in ["BTC_scaler_X", "BTC_scaler_y"]:
    scaler_path = f"d:\\Study\\Python\\Crypto_AI_prj2\\Airflow\\dags\\models\\{name}.pkl"
    scaler = joblib.load(scaler_path)

    data = {
        "min_": scaler.min_.tolist(),
        "scale_": scaler.scale_.tolist(),
        "data_min_": scaler.data_min_.tolist(),
        "data_max_": scaler.data_max_.tolist(),
    }

    with open(f"d:\\Study\\Python\\Crypto_AI_prj2\\Airflow\\dags\\models\\{name}.json", "w") as f:
        json.dump(data, f)
    
    print(f"Saved {name}.json")
