
import os
import pandas as pd
import numpy as np
from scipy.ndimage import gaussian_filter1d
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import pyodbc

# === Paths and Parameters ===
mdb_dir = r'C:\Users\user\Desktop\new full days measurments'
mdb_files = [f for f in os.listdir(mdb_dir) if f.endswith('.mdb')]
table_name = 'RT Data Log #18_1'
time_column = 'StringTime'
timestamp_column = 'timestamp'
known_devices_path = os.path.join(mdb_dir, 'known_devices.xlsx')

# === Load Known Devices ===
known_devices = pd.read_excel(known_devices_path)
known_devices.columns = known_devices.columns.str.strip()
known_device_names = set(known_devices['device name'])

# === Load MDB File ===
def load_mdb_data(mdb_path, table):
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={mdb_path};'
    )
    conn = pyodbc.connect(conn_str)
    query = f"SELECT * FROM [{table}]"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# === Detect Edges ===
def detect_edges(data, param, threshold, phase_label, param_type, stabilize_threshold, all_params, k=2, stabilize_window_seconds=3):
    edges = []
    i = k
    while i < len(data) - k:
        diff_prev = abs(data[param].iloc[i] - data[param].iloc[i - k])
        diff_next = abs(data[param].iloc[i + k] - data[param].iloc[i])
        if diff_prev > threshold or diff_next > threshold:
            start_time = data['timestamp'].iloc[i]
            start_values = {p: data[p].iloc[i] for p in all_params if p in data.columns}
            j = i + 1
            while j < len(data) - 1:
                stable = True
                end_idx = j
                while end_idx < len(data) - 1:
                    if (data['timestamp'].iloc[end_idx] - data['timestamp'].iloc[j]).total_seconds() >= stabilize_window_seconds:
                        break
                    diff = abs(data[param].iloc[end_idx + 1] - data[param].iloc[end_idx])
                    if diff >= stabilize_threshold:
                        stable = False
                        break
                    end_idx += 1
                if stable:
                    break
                j = end_idx + 1
            end_time = data['timestamp'].iloc[j]
            end_values = {p: data[p].iloc[j] for p in all_params if p in data.columns}
            total_change = abs(end_values[param] - start_values[param])
            if total_change > threshold:
                param_changes = {f"Δ{p}": end_values[p] - start_values[p] for p in all_params if p in data.columns}
                edges.append({
                    'start_timestamp': start_time,
                    'end_timestamp': end_time,
                    'phase': phase_label,
                    'parameter': param_type,
                    'jump_magnitude': total_change,
                    'param_changes': param_changes,
                    'file': data['file'].iloc[0]
                })
            i = j + 1
        else:
            i += 1
    return edges

# === Phase Info ===
phases_info = [
    {'power': 'kW L1', 'current': 'I1', 'label': 'Phase L1', 'all_params': ['I1', 'I1 Ang', 'I1 THD', 'kW L1', 'kvar L1', 'PF L1', 'I1 TDD', 'I1 %HD03', 'I1 %HD05', 'I1 %HD07', 'I1 KF']},
    {'power': 'kW L2', 'current': 'I2', 'label': 'Phase L2', 'all_params': ['I2', 'I2 Ang', 'I2 THD', 'kW L2', 'kvar L2', 'PF L2', 'I2 TDD', 'I2 %HD03', 'I2 %HD05', 'I2 %HD07', 'I2 KF']},
    {'power': 'kW L3', 'current': 'I3', 'label': 'Phase L3', 'all_params': ['I3', 'I3 Ang', 'I3 THD', 'kW L3', 'kvar L3', 'PF L3', 'I3 TDD', 'I3 %HD03', 'I3 %HD05', 'I3 %HD07', 'I3 KF']},
]

# === Thresholds ===
power_threshold = 0.2
current_threshold = 0.5
power_stabilize_threshold = 0.1
current_stabilize_threshold = 0.2

# === Detect All Events ===
all_edges = []
all_data_by_file = {}

for mdb_file in mdb_files:
    path = os.path.join(mdb_dir, mdb_file)
    df = load_mdb_data(path, table_name)
    df[time_column] = df[time_column].replace(r'^\s*$', pd.NA, regex=True).str.strip()
    df['timestamp'] = pd.to_datetime(df[time_column], format='%m/%d/%y %H:%M:%S.%f', errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df['file'] = mdb_file
    all_data_by_file[mdb_file] = df.copy()

    for phase in phases_info:
        if not all(col in df.columns for col in [phase['power'], phase['current']] + phase['all_params']):
            continue
        df[phase['power']] = gaussian_filter1d(df[phase['power']], sigma=1)
        df[phase['current']] = gaussian_filter1d(df[phase['current']], sigma=1)
        power_edges = detect_edges(df, phase['power'], power_threshold, phase['label'], 'Power', power_stabilize_threshold, phase['all_params'])
        current_edges = detect_edges(df, phase['current'], current_threshold, phase['label'], 'Current', current_stabilize_threshold, phase['all_params'])
        all_edges.extend(power_edges + current_edges)

# === Merge Events ===
merged_events = []
for event in all_edges:
    matched = False
    for merged in merged_events:
        same_phase = merged['Phase'] == event['phase']
        same_file = merged['File'] == event['file']
        overlap = (min(merged['End Timestamp'], event['end_timestamp']) - max(merged['Start Timestamp'], event['start_timestamp'])).total_seconds()
        gap = abs((merged['Start Timestamp'] - event['end_timestamp']).total_seconds())
        if same_phase and same_file and (overlap >= 0 or gap <= 2):
            merged['Parameters'].add(event['parameter'])
            merged['param_changes'].update(event['param_changes'])
            merged['Start Timestamp'] = min(merged['Start Timestamp'], event['start_timestamp'])
            merged['End Timestamp'] = max(merged['End Timestamp'], event['end_timestamp'])
            matched = True
            break
    if not matched:
        merged_events.append({
            'File': event['file'],
            'Start Timestamp': event['start_timestamp'],
            'End Timestamp': event['end_timestamp'],
            'Phase': event['phase'],
            'Parameters': set([event['parameter']]),
            'param_changes': event['param_changes'].copy()
        })

# === Create DataFrame ===
rows = []
for merged in merged_events:
    row = {
        'File': merged['File'],
        'Start Timestamp': merged['Start Timestamp'],
        'End Timestamp': merged['End Timestamp'],
        'Phase': merged['Phase'],
        'Detected Parameters': ', '.join(sorted(merged['Parameters']))
    }
    for param, val in merged['param_changes'].items():
        row[param] = round(val, 4) if isinstance(val, (float, int)) else val
    rows.append(row)

events_df = pd.DataFrame(rows)

# === Clustering ===
phases = [
    {'label': 'Phase L1', 'cluster_params': ['ΔkW L1', 'Δkvar L1']},
    {'label': 'Phase L2', 'cluster_params': ['ΔkW L2', 'Δkvar L2']},
    {'label': 'Phase L3', 'cluster_params': ['ΔkW L3', 'Δkvar L3']}
]
phase_eps = {'Phase L1': 0.2, 'Phase L2': 0.18, 'Phase L3': 0.2}

for phase in phases:
    label = phase['label']
    cluster_cols = phase['cluster_params']
    phase_df = events_df[events_df['Phase'] == label].copy()
    if phase_df.empty or not all(col in phase_df.columns for col in cluster_cols):
        continue
    cluster_data = phase_df[cluster_cols].fillna(0)
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(cluster_data)
    eps_value = phase_eps.get(label, 0.35)
    dbscan = DBSCAN(eps=eps_value, min_samples=2)
    labels = dbscan.fit_predict(scaled_data)
    events_df.loc[events_df['Phase'] == label, 'Cluster'] = labels

# === שיוך ראשוני של אירועים למכשירים על פי חתימה (ΔkW, Δkvar) ===
def identify_device(row):
    phase = row['Phase']
    suffix = phase[-2:]
    p = row.get(f'ΔkW {suffix}', 0)
    q = row.get(f'Δkvar {suffix}', 0)

    candidates = []
    for _, dev in known_devices.iterrows():
        dev_phase = dev['Phase'].strip()
        if dev_phase != phase:
            continue
        eps = dev['eps']
        dp = abs(dev['ΔkW'] - p)
        dq = abs(dev['Δkvar'] - q)

        # זיהוי רגיל
        if dp <= eps and dq <= eps:
            score = dp + dq
            candidates.append((score, dev['device name']))

        # זיהוי הופכי רק אם OFF לא קיים מראש
        if "ON" in dev['device name']:
            reversed_name = dev['device name'].replace("ON", "OFF")
            if reversed_name not in known_device_names:
                dp_rev = abs(dev['ΔkW'] + p)
                dq_rev = abs(dev['Δkvar'] + q)
                if dp_rev <= eps and dq_rev <= eps:
                    score = dp_rev + dq_rev
                    candidates.append((score, reversed_name))

    if candidates:
        candidates.sort()
        return candidates[0][1]
    else:
        return 'UNKNOWN'


# === שלב השיוך הראשוני ===
events_df['Device'] = events_df.apply(identify_device, axis=1)

# === Identify Devices Using Phase+Cluster Memory ===
cluster_to_device = {}

# שלב 1: שמירת שיוך של קלאסטרים לזיהוי הכי שכיח (למעט UNKNOWN)
# שלב 1: שיוך חכם לקלאסטר — אם יש התאמה כמעט מושלמת (≥99%) למכשיר מוכר, היא תקבע
for phase in events_df['Phase'].unique():
    phase_df = events_df[events_df['Phase'] == phase]
    for cluster_id in phase_df['Cluster'].dropna().unique():
        if cluster_id == -1:
            continue  # דלג על קלאסטרים שהם רעש

        cluster_df = phase_df[phase_df['Cluster'] == cluster_id]

        best_match_device = None
        for idx, row in cluster_df.iterrows():
            suffix = phase[-2:]
            p = row.get(f'ΔkW {suffix}', 0)
            q = row.get(f'Δkvar {suffix}', 0)

            for _, dev in known_devices.iterrows():
                if dev['Phase'].strip() != phase:
                    continue
                eps = dev['eps']
                dp = abs(dev['ΔkW'] - p)
                dq = abs(dev['Δkvar'] - q)
                match_score = 1 - (dp + dq) / (2 * eps)  # מדד התאמה בין 0 ל-1

                if match_score >= 0.99:
                    best_match_device = dev['device name']
                    break  # מספיק אחד!

            if best_match_device:
                break  # מספיק אירוע אחד כדי לשייך את כל הקלאסטר

        if best_match_device:
            cluster_to_device[(phase, cluster_id)] = best_match_device
        else:
            # fallback לרוב קלאסי
            device_counts = cluster_df['Device'].value_counts() if 'Device' in cluster_df else pd.Series()
            device_counts = device_counts[device_counts.index != 'UNKNOWN']
            if not device_counts.empty:
                cluster_to_device[(phase, cluster_id)] = device_counts.idxmax()



# שלב 2: שיוך מחדש על פי Phase+Cluster
for (phase, cluster_id), device in cluster_to_device.items():
    mask = (events_df['Phase'] == phase) & (events_df['Cluster'] == cluster_id)
    events_df.loc[mask, 'Device'] = device

# === חיזוק למייבש בפאזה 1 ===
def strengthen_dryer(row):
    if row['Phase'] == 'Phase L1' and row.get('Device') == 'dryer ON':
        p = row.get('ΔkW L1', 0)
        q = row.get('Δkvar L1', 0)
        if not (0.175 <= p <= 0.21 and 0.038 <= q <= 0.048):
            return 'UNKNOWN'
    return row.get('Device', 'UNKNOWN')

events_df['Device'] = events_df.apply(strengthen_dryer, axis=1)

# === חיזוק טאמי מול מדיח ===
def refine_tami_vs_dishwasher(events_df):
    df = events_df[events_df['Phase'] == 'Phase L2'].copy()
    on_mask = df['Device'].isin(['dishwasher ON', 'tami ON'])
    off_mask = df['Device'].isin(['dishwasher OFF', 'tami OFF'])
    on_events = df[on_mask].sort_values('Start Timestamp')
    off_events = df[off_mask].sort_values('Start Timestamp')

    used_off_indices = set()

    for on_idx, on_row in on_events.iterrows():
        on_time = on_row['Start Timestamp']
        file = on_row['File']
        matching_off = off_events[
            (off_events['File'] == file) &
            (off_events['Start Timestamp'] > on_time) &
            (off_events['Start Timestamp'] <= on_time + timedelta(minutes=3)) &
            (~off_events.index.isin(used_off_indices))
        ]
        if not matching_off.empty:
            off_idx = matching_off.index[0]
            events_df.at[on_idx, 'Device'] = 'tami ON'
            events_df.at[off_idx, 'Device'] = 'tami OFF'
            used_off_indices.add(off_idx)
        else:
            events_df.at[on_idx, 'Device'] = 'dishwasher ON'

    # כל OFF שלא שויך עדיין - שייך למדיח
    for off_idx in off_events.index:
        if off_idx not in used_off_indices:
            events_df.at[off_idx, 'Device'] = 'dishwasher OFF'

refine_tami_vs_dishwasher(events_df)

# === חיזוק מכונת כביסה ודניאל בפאזה 3 ===
def refine_phase3_logic(df):
    for idx, row in df[df['Phase'] == 'Phase L3'].iterrows():
        p = row.get('ΔkW L3', 0)
        label = row['Device']
        if label == 'daniel aircon OFF':
            if abs(p + 0.1) < abs(p + 0.3):
                df.at[idx, 'Device'] = 'washing machine 3'
        if label == 'washing machine 1' and p < 0:
            df.at[idx, 'Device'] = 'washing machine 2'
    return df

events_df = refine_phase3_logic(events_df)

def annotate_off_events_with_price(events_df, price_per_kwh=0.6402):
    events_df = events_df.copy()
    events_df['Price including VAT'] = np.nan

    for phase in events_df['Phase'].unique():
        df_phase = events_df[events_df['Phase'] == phase].copy()
        on_mask = df_phase['Device'].str.endswith('ON', na=False)
        off_mask = df_phase['Device'].str.endswith('OFF', na=False)

        ons = df_phase[on_mask].sort_values('Start Timestamp')
        offs = df_phase[off_mask].sort_values('Start Timestamp')

        used_on_indices = set()

        for off_idx, off_row in offs.iterrows():
            device_name = off_row['Device'][:-4].strip()  # בלי " OFF"
            off_time = off_row['Start Timestamp']
            suffix = phase[-2:]

            matching_ons = ons[
                (ons['Device'].str.startswith(device_name)) &
                (ons['Start Timestamp'] < off_time) &
                (~ons.index.isin(used_on_indices))
            ]

            if not matching_ons.empty:
                on_idx = matching_ons.index[-1]  # הכי קרוב לפני
                on_row = matching_ons.loc[on_idx]
                used_on_indices.add(on_idx)

                duration_hr = (off_time - on_row['Start Timestamp']).total_seconds() / 3600.0
                p = abs(on_row.get(f'ΔkW {suffix}', 0))  # ערך מוחלט של ההספק
                price = round(p * duration_hr * price_per_kwh, 3)

                events_df.at[off_idx, 'Price including VAT'] = price

    return events_df
events_df = annotate_off_events_with_price(events_df, price_per_kwh=0.6402)


# === Save Results ===
output_path = os.path.join(mdb_dir, 'classified_events_final.xlsx')
events_df.to_excel(output_path, index=False)
print(f"Saved to {output_path}")

