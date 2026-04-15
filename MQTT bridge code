import os
import json
import time
import shutil
import numpy as np
import scipy.io as sio
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from collections import defaultdict
from queue import Queue
import threading

# This MQTT bridge code listens to temperature, voltage, current and SE input messages(from publisher),
# caches measurement data, and triggers SE processing tasks. It organ-izes data by timestamp into folders, 
# saves .mat files for Voltage, Current, and Temperature,and Bus, Line and others
# and ensures only the newest folders are kept. 
# The system matches measurements to the closest SE task timestamp, runs tasks in a worker thread, and processes each timestamp only once(no duplicates). 
# It automatically reconnects to the MQTT broker and continuously han-dles incoming messages. 

INPUT_DIR = "input_data"
MAX_FOLDERS = 3

# Initialize caches for temperature, voltage, and current, 
temperature_cache = None
voltage_cache = defaultdict(dict)
current_cache = defaultdict(dict)

# and the SE task queue and a set to track processed timestamps
se_queue = Queue()
processed_timestamps = set()

# Deletes the oldest folders to ensure the total number stays within MAX_FOLDERS in input folder
def clean_old_folders(base_path):
    folders = sorted([f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))])
    while len(folders) > MAX_FOLDERS:
        shutil.rmtree(os.path.join(base_path, folders.pop(0)))

# Extracts the BusID from an MQTT topic string
def extract_bus_or_branch_id(topic):
    parts = topic.strip("/").split("/")
    if "Bus" in parts:
        idx = parts.index("Bus")
        return parts[idx + 1] if idx + 1 < len(parts) else None
    if "Branch" in parts:
        idx = parts.index("Branch")
        return parts[idx + 1] if idx + 1 < len(parts) else None
    return None

# Normalizes timestamp strings by correcting time formats and timezone parts
def normalize_timestamp(ts): 
    if 'T' in ts:
        date_part, time_part = ts.split('T', 1)
        for tz_start in ['Z', '+', '-']:
            if tz_start in time_part:
                idx = time_part.index(tz_start)
                time_only = time_part[:idx].replace('-', ':')
                tz = time_part[idx:].replace('+00-00', '+00:00').replace('-00-00', '-00:00')
                return f"{date_part}T{time_only}{tz}"
        return f"{date_part}T{time_part.replace('-', ':')}"
    return ts

# Cleans MessageId timestamps by normalizing and fixing malformed 'Z-' endings
def clean_msgid(ts):
    ts = normalize_timestamp(ts)
    return ts.split('Z')[0] + 'Z' if 'Z-' in ts else ts

# Finds the cached timestamp closest to the given timestamp, within a tolerance (seconds)
def find_closest_cached(cache, ts, tolerance=70):
    try:
        ts_norm = ts
        if ts_norm.endswith('Z'):
            ts_norm = ts_norm[:-1] + '+00:00'
        target = datetime.fromisoformat(ts_norm)
        for cached_ts in cache:
            cached_norm = cached_ts
            if cached_norm.endswith('Z'):
                cached_norm = cached_norm[:-1] + '+00:00'
            cached_dt = datetime.fromisoformat(cached_norm)
            delta = abs((target - cached_dt).total_seconds())
            if delta <= tolerance:
                return cached_ts
    except Exception as e:
        print(f"[ERROR] find_closest_cached failed: {e}")
    return None

# Returns dictionary items sorted by key, converting keys to integers when possible
def sorted_items(d):
    try:
        return sorted(d.items(), key=lambda x: int(x[0]))
    except:
        return sorted(d.items())

# Truncates a timestamp to the nearest minute and formats it consist-ently
def truncate_to_minute(ts):
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dt = dt.replace(second=0, microsecond=0)
        return dt.isoformat().replace(":", "-") + "Z"
    except Exception as e:
        print(f"[ERROR] Truncating timestamp failed: {e}")
        return ts.replace(":", "-")  # fallback
    
# Prepares SE task by cleaning timestamp, creating the input folder, and setting up paths
def process_se_task(timestamp, data):
    global temperature_cache, voltage_cache, current_cache

    timestamp = clean_msgid(timestamp)
    timestamp_filename = truncate_to_minute(timestamp)
    input_path = os.path.join(INPUT_DIR, timestamp_filename)

    os.makedirs(input_path, exist_ok=True)

    # Save arrival timestamp for latency calculation (message arrival time)
    arrival_time = data.get('_arrival_time', None)
    if arrival_time:
        with open(os.path.join(input_path, "arrival_time.txt"), "w") as f:
            f.write(str(arrival_time))

    try: #saves the data from publisher(mqtt_test_input.py], Load-flow.py in .mat 
        print(f"Processing timestamp: {timestamp}")
        print(f"[Voltage Cache] Keys: {list(voltage_cache.keys())}")
        print(f"[Current Cache] Keys: {list(current_cache.keys())}")

        if "Bus" in data and data["Bus"]:
            sio.savemat(os.path.join(input_path, "Bus.mat"), {"Bus": np.array(data["Bus"])})
        if "Line" in data and data["Line"]:
            sio.savemat(os.path.join(input_path, "Line.mat"), {"Line": np.array(data["Line"])})
        if "BaseVoltage" in data:
            sio.savemat(os.path.join(input_path, "BaseVoltage.mat"), {"BaseVoltage": np.array(data["BaseVoltage"])})
        for key in ["Pkm", "Qkm", "Ik", "Pk", "Qk"]:
            sio.savemat(os.path.join(input_path, f"{key}.mat"), {key: np.array(data.get(key, []))})
        sio.savemat(os.path.join(input_path, "Sbase.mat"), {"Sbase": np.array([data.get("Sbase", 1.0)])})

        # Try to find the closest voltage and current timestamps to the target 'timestamp'
        closest_voltage_ts = None
        closest_current_ts = None

        #We retry up to 5 times because sometimes voltage and current don't arrive at the same time
        for _ in range(5):
            # Find the timestamp in the cache that is closest to the given timestamp
            closest_voltage_ts = find_closest_cached(voltage_cache, timestamp)
            closest_current_ts = find_closest_cached(current_cache, timestamp)
            print(f"[SE Task] Closest Voltage TS: {clos-est_voltage_ts}, Closest Current TS: {closest_current_ts}")

            # If both timestamps were found, no need to retry further
            if closest_voltage_ts and closest_current_ts:
                break
            time.sleep(1) # Wait 1 second before checking again

        # ------------------------------- Voltage Saving Section ----------------
        # If a closest voltage timestamp was found AND that entry has actual data
        if closest_voltage_ts and voltage_cache[closest_voltage_ts]: 
            V_array = np.array([v for _, v in sort-ed_items(voltage_cache[closest_voltage_ts])], dtype=float) # Convert all voltage values (sorted by key) into a NumPy array
            sio.savemat(os.path.join(input_path, "Voltage.mat"), {"Voltage": V_array}) # Save the NumPy array into a .mat file Volt-age.mat
            voltage_cache.pop(closest_voltage_ts, None) # Remove this timestamp from the cache since it's already processed
            print(f"Saved Voltage.mat for {closest_voltage_ts}")
        else:
            print("No voltage data to save")

        # ------------------------------- Current Saving Section -------------------
        # Same process as voltage, but for current data
        if closest_current_ts and current_cache[closest_current_ts]:
            I_array = np.array([i for _, i in sort-ed_items(current_cache[closest_current_ts])], dtype=float)
            sio.savemat(os.path.join(input_path, "Current.mat"), {"Current": I_array})
            current_cache.pop(closest_current_ts, None)
            print(f"Saved Current.mat for {closest_current_ts}")
        else:
            print("No current data to save")
        
        # ------------------------------- temperature saving Section -------------------
        # Convert temperature to float if valid, otherwise set as NaN
        temp_val = float(temperature_cache) if temperature_cache is not None and not np.isnan(temperature_cache) else np.nan
        sio.savemat(os.path.join(input_path, "Temperature.mat"), {"Temperature": temp_val})# Save the temperature value into Tempera-ture.mat

        clean_old_folders(INPUT_DIR) # Remove old input folders to keep clean

    except Exception as e:
        print(f"[ERROR] SE task failed for {timestamp}: {e}")
    finally:
        processed_timestamps.add(timestamp) # Mark this timestamp as processed no matter what

# Continuously reads tasks from the queue and processes each SE task once per timestamp.
def se_worker():
    while True:
        timestamp, data = se_queue.get()
        if timestamp in processed_timestamps:
            se_queue.task_done()
            continue
        process_se_task(timestamp, data)
        se_queue.task_done()

# Handles MQTT connection event and subscribes to all required topics when connected.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("se/input")
        client.subscribe("Measurement/Temperature")
        client.subscribe("A-T/Measurement/Voltage/kV/RMS/1min/+/PhaseA/Bus/+/")
        client.subscribe("A-T/Measurement/Current/Ampere/RMS/1min/+/PhaseA/Branch/+/")
    else:
        print(f"Connection failed: {rc}")

def on_message(client, userdata, msg):
    global temperature_cache

    topic = msg.topic
    payload_str = msg.payload.decode()

    if topic == "Measurement/Temperature": # Handle incoming tempera-ture messages
        try:
            data = json.loads(payload_str)
            temp = data.get("Temperature", np.nan)
            if isinstance(temp, dict):
                temp = temp.get("Value", np.nan)
            temperature_cache = float(temp)
            print(f"[Temperature Cache] Updated: {temperature_cache}")
        except Exception as e:
            print(f"[ERROR] Parsing temperature message: {e}")
        return

    if "voltage" in topic.lower() or "current" in topic.lower(): # Handle incoming voltage or current measurement messages
        try:
            data = json.loads(payload_str)
            value = float(data["Value"])
            timestamp = clean_msgid(data["MessageId"])

            if "voltage" in topic.lower():
                bus_id = extract_bus_or_branch_id(topic)
                if bus_id:
                    voltage_cache[timestamp][bus_id] = value
                    print(f"[Voltage Cache] Added: ts={timestamp}, bus={bus_id}, value={value}")
            elif "current" in topic.lower():
                branch_id = extract_bus_or_branch_id(topic)
                if branch_id:
                    current_cache[timestamp][branch_id] = value
                    print(f"[Current Cache] Added: ts={timestamp}, branch={branch_id}, value={value}")
        except Exception as e:
            print(f"[ERROR] Parsing voltage/current message: {e}")
        return

    if topic == "se/input": # Handle SE input messages that trigger new SE tasks
        try:
            arrival_time = time.time()  # Capture arrival timestamp
            data = json.loads(payload_str)
            timestamp = clean_msgid(data.get("MessageId", datetime.utcnow().isoformat() + "Z"))
            if timestamp not in processed_timestamps:
                print(f"[SE Input] New SE task queued for {timestamp}")
                # Add arrival time to data for latency tracking
                data['_arrival_time'] = arrival_time
                se_queue.put((timestamp, data))
                
        except Exception as e:
            print(f"[ERROR] Parsing se/input message: {e}")

# Sets up MQTT client, starts the SE worker thread, retries connection if needed
# and runs the MQTT loop to process incoming messages
def connect_mqtt_with_retry(client, host, port, max_retries=10, de-lay=3):
    for _ in range(max_retries):
        try:
            client.connect(host, port, 60)
            return True
        except Exception as e:
            print(f"Retrying MQTT connection: {e}")
            time.sleep(delay)
    return False

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

threading.Thread(target=se_worker, daemon=True).start()

if connect_mqtt_with_retry(client, "rabbitmq_broker", 1883):
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("Interrupted")
else:
    print("Failed to connect to MQTT broker")
