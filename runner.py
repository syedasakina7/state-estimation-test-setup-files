'''
This runner continuously monitors the `input_data`
directory for new timestamped folders created by the MQTT
bridge. Each folder represents a new SE task. When a new
folder is detected, the script:

1. Runs the Octave SE master code for that folder.
2. Logs performance timestamps:
       - T2: SE computation start time
       - T3: SE result publish time
3. Loads SE result files (`V.mat`, `Iline_actual.mat`, `Sloss.mat`) and converts them into JSON-friendly format (handles complex values).
4. Publishes the SE results to the MQTT topic `se/output`.
5. Saves a local copy as `result.json` inside the output folder.
6. Cleans old input and output folders to keep only recent ones.

'''

import os
import time
import shutil
import scipy.io as sio
import subprocess
import json
from datetime import datetime
import paho.mqtt.client as mqtt

print("[DEBUG] Python working directory:", os.getcwd())

INPUT_DIR = "input_data"
OUTPUT_DIR = "output_data"
MAX_FOLDERS = 3

# MQTT settings
MQTT_BROKER = "rabbitmq_broker"
MQTT_PORT = 1883
MQTT_TOPIC = "se/output"

# Initialize MQTT client
mqtt_client = mqtt.Client()

# Tries multiple times to connect to the MQTT broker, waiting between attempts 
def connect_mqtt_with_retry(client, host, port, max_retries=10, de-lay=3):
    for attempt in range(max_retries):
        try:
            client.connect(host, port, 60)
            return True
        except Exception as e:
            print(f"[MQTT] Connection attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    return False

# Removes the oldest folders in the given directory to keep only the newest MAX_FOLDERS
def clean_old_folders(base_path):
    if not os.path.exists(base_path):
        return
    folders = sorted([f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))])
    while len(folders) > MAX_FOLDERS:
        old_folder = folders.pop(0)
        full_path = os.path.join(base_path, old_folder)
        print(f"[Cleanup] Removing old folder: {full_path}")
        shutil.rmtree(full_path)

# Converts a folder name back into a properly formatted ISO timestamp (fixes time separators and ensures valid format)
def normalize_folder_name(folder_name):
    try:
        parts = folder_name.split('T')
        if len(parts) != 2:
            return folder_name
        date_part, time_part = parts
        time_part = time_part.replace('-', ':', 2)
        ts = f"{date_part}T{time_part}"
        dt = datetime.fromisoformat(ts.replace('Z', ''))
        return dt.replace(microsecond=0).isoformat() + 'Z'
    except Exception:
        return folder_name

def process_se_task(timestamp_folder): #sets up the directories where the SE results will be stored for that specific timestam
    input_path = os.path.join(INPUT_DIR, timestamp_folder)
    output_path = os.path.join(OUTPUT_DIR, timestamp_folder)

    os.makedirs(output_path, exist_ok=True)

    try:
        print(f"[SE] Running SE for folder: {timestamp_folder}")

        # === PERFORMANCE LOGGING: T2 (SE Computation Start) ===
        t2 = time.time()
        with open(os.path.join(output_path, "T2_start_time.txt"), "w") as f:
            f.write(str(t2))
        print(f"[PERF_LOG] T2 logged at {t2} for {timestamp_folder}")
        
         # Run the octave SE code (se_master.m)
        result = subprocess.run(
            ["octave", "--quiet", "se_master.m", timestamp_folder],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Print octave's output for debugging -
        print("\n===== OCTAVE STDOUT =====")
        print(result.stdout)
        print("===== OCTAVE STDERR =====")
        print(result.stderr)
        print("==========================\n")
        time.sleep(2)

        print(f"[DEBUG] SE subprocess completed for {timestamp_folder}")
        print(f"[DEBUG] Output path: {output_path}")
        print(f"[DEBUG] Output path exists: {os.path.exists(output_path)}")

        if os.path.exists(output_path):
            print(f"[DEBUG] Files in output folder: {os.listdir(output_path)}")

        V_path = os.path.join(output_path, "V.mat")
        I_path = os.path.join(output_path, "Iline_actual.mat")
        Sloss_path = os.path.join(output_path, "Sloss.mat")

        print(f"[DEBUG] V.mat exists: {os.path.exists(V_path)}")
        print(f"[DEBUG] Iline_actual.mat exists: {os.path.exists(I_path)}")
        print(f"[DEBUG] Sloss.mat exists: {os.path.exists(Sloss_path)}")

        # Load SE result files if all exist
        if all(os.path.exists(p) for p in [V_path, I_path, Sloss_path]):
            print(f"[DEBUG] All output files exist, proceeding to load...")
            V = sio.loadmat(V_path)["V"]
            Iline = sio.loadmat(I_path)["Iline_actual"]
            Sloss = sio.loadmat(Sloss_path)["Sloss"]

            # Convert complex arrays to JSON-serializable format
            def complex_to_dict(arr):
                if arr.dtype == complex:
                    return {
                        "real": arr.real.tolist(),
                        "imag": arr.imag.tolist()
                    }
                else:
                    return arr.tolist()
            
            result = {
                "V": complex_to_dict(V),
                "Iline": complex_to_dict(Iline),
                "Sloss": complex_to_dict(Sloss)
            }
            
            print(f"Completed SE for {timestamp_folder}")
            print(f"Publishing result to MQTT...")

            # Publish SE results to MQTT
            mqtt_client.publish(MQTT_TOPIC, json.dumps(result))
            print(f"[MQTT] Published result for {timestamp_folder}")

            # ==PERFORMANCE LOGGING: T3 (SE Result Published) ===
            t3 = time.time()
            with open(os.path.join(output_path, "T3_publish_time.txt"), "w") as f:
                f.write(str(t3))
            print(f"[PERF_LOG] T3 logged at {t3} for {timestamp_folder}")

            with open(os.path.join(output_path, "result.json"), "w") as f:
                json.dump(result, f, indent=2)

        else:
            print(f"[WARNING] Output files missing for {timestamp_folder}")

    except Exception as e:
        print(f"[ERROR] SE failed for {timestamp_folder}: {e}")

     # Cleanup: keep only recent folders in input/output directories
    clean_old_folders(INPUT_DIR)
    clean_old_folders(OUTPUT_DIR)

def watch_input_folder():
    print("[Runner] Watching input folder...")
    processed_folders = set()

    while True: # read list of folders inside INPUT_DIR
        try:
            all_folders = sorted([
                f for f in os.listdir(INPUT_DIR)
                if os.path.isdir(os.path.join(INPUT_DIR, f))
            ])
        except Exception as e:
            print(f"[ERROR] Could not list input folders: {e}")
            time.sleep(5)
            continue
        
        #  Detect new folders and trigger SE processing
        for folder in all_folders:
            if folder not in processed_folders:
                print(f"[Runner] New folder detected: {folder}")
                processed_folders.add(folder)
                process_se_task(folder)

        time.sleep(0.5) # Small delay before next scan

if __name__ == "__main__":
    print("[Runner] State Estimation runner started.")

    if connect_mqtt_with_retry(mqtt_client, MQTT_BROKER, MQTT_PORT):
        watch_input_folder()
    else:
        print("[MQTT] Failed to connect. Exiting.")
