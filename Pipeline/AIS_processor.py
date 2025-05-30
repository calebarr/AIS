import os
from datetime import datetime, timedelta
import requests
import zipfile
import pandas as pd

class AISPortVisitProcessor:
    """
    Handles downloading AIS data, processing it to identify port visits,
    and saving results to CSV. Simplified to one visit per vessel.
    """

    PORT_REGIONS = {
        "Los Angeles": (33.6, 33.9, -118.5, -118.0),
        "Long Beach": (33.7, 33.9, -118.25, -118.15),
        "Oakland": (37.7, 37.85, -122.35, -122.2),
        "Seattle": (47.5, 47.7, -122.4, -122.2),
        "New York": (40.6, 40.8, -74.1, -73.9),
        "Norfolk": (36.8, 37.1, -76.4, -76.2),
        "Savannah": (32.0, 32.2, -81.2, -80.8),
        "Charleston": (32.7, 32.9, -80.0, -79.8),
        "Miami": (25.75, 25.85, -80.2, -80.0),
        "Port Everglades": (26.05, 26.1, -80.15, -80.1),
        "Baltimore": (39.2, 39.3, -76.6, -76.5),
        "Philadelphia": (39.9, 40.0, -75.2, -75.1),
        "Houston": (29.6, 29.8, -95.2, -94.8),
        "New Orleans": (29.9, 30.1, -90.1, -89.9),
        "Jacksonville": (30.3, 30.5, -81.7, -81.3),
        "San Diego": (32.7, 32.8, -117.2, -117.1),
        "Boston": (42.3, 42.4, -71.1, -70.9),
        "Anchorage": (61.1, 61.3, -149.95, -149.8),
        "Honolulu": (21.3, 21.4, -157.9, -157.8),
        "Portland": (45.6, 45.7, -122.7, -122.6),
        "Puerto Rico": (18.2, 18.3, -66.3, -66.2),
        "Tacoma": (47.2, 47.4, -122.55, -122.35),
        "Port Arthur": (29.85, 29.95, -93.95, -93.85),
        "Beaumont": (30.0, 30.1, -94.15, -94.05),
        "Corpus Christi": (27.75, 27.9, -97.45, -97.25),
        "Baton Rouge": (30.4, 30.5, -91.25, -91.15),
        "Mobile": (30.6, 30.7, -88.1, -88.0),
        "Tampa": (27.9, 28.0, -82.5, -82.4),
        "San Francisco": (37.75, 37.85, -122.45, -122.3),
        "Wilmington (DE)": (39.7, 39.75, -75.55, -75.5),
        "Camden (NJ)": (39.9, 39.95, -75.1, -75.05),
        "Providence": (41.7, 41.8, -71.45, -71.35),
        "Unknown": (None, None, None, None),  # fallback
    }

    def __init__(self, buffer_degrees: float = 1.3):
        self.buffer = buffer_degrees

    def get_port_name(self, lat: float, lon: float) -> str:
        """Return the port name for given coordinates or 'Unknown'."""
        for port, bounds in self.PORT_REGIONS.items():
            if port == "Unknown":
                continue
            min_lat, max_lat, min_lon, max_lon = bounds
            if (min_lat - self.buffer) <= lat <= (max_lat + self.buffer) and \
               (min_lon - self.buffer) <= lon <= (max_lon + self.buffer):
                return port
        return "Unknown"

    def assign_port_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Annotate each AIS record with its port based on LAT/LON."""
        df = df.copy()
        df['Port Name'] = df.apply(
            lambda row: self.get_port_name(row['LAT'], row['LON']), axis=1
        )
        return df

    def extract_relevant_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter AIS pings by vessel type, valid positions, and anchored status."""
        df = df[
            df['VesselType'].isin(range(70, 90)) |
            df['VesselType'].isin([30, 52])
        ]
        df = df.dropna(subset=['LAT', 'LON'])
        df = df[(df['LAT'] != 0) & (df['LON'] != 0)]
        df['BaseDateTime'] = pd.to_datetime(df['BaseDateTime'], errors='coerce')
        df = df.dropna(subset=['BaseDateTime'])
        if 'Status' in df.columns:
            df = df[df['Status'].isin([1, 5])]
        drop_cols = [c for c in [
            "SOG", "COG", "Heading", "IMO", "VesselName",
            "Length", "Width", "TransceiverClass", "Cargo",
            "CallSign", "Draft"
        ] if c in df.columns]
        return df.drop(columns=drop_cols)

    def identify_port_visit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Simplified: for each vessel (MMSI), find first arrival in a port,
        then first departure (first 'Unknown' after arrival), one row per MMSI.
        """
        visits = []
        for mmsi, group in df.groupby('MMSI'):
            grp = group.sort_values('BaseDateTime')
            # arrival: first in-port
            in_port = grp[grp['Port Name'] != 'Unknown']
            if in_port.empty:
                continue
            arrival_row = in_port.iloc[0]
            arrival_time = arrival_row['BaseDateTime']
            port = arrival_row['Port Name']
            # departure: first unknown after arrival
            after_arr = grp[grp['BaseDateTime'] > arrival_time]
            departed = after_arr[after_arr['Port Name'] == 'Unknown']
            departure_time = departed.iloc[0]['BaseDateTime'] if not departed.empty else None
            visits.append({
                'MMSI': mmsi,
                'Port Name': port,
                'Arrival': arrival_time,
                'Departure': departure_time
            })
        return pd.DataFrame(visits)

    def process_zip(self, zip_path: str, chunksize: int = 100_000) -> pd.DataFrame:
        """Process one ZIP file, extracting a single visit per vessel."""
        frames = []
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csvs = [n for n in zf.namelist() if n.endswith('.csv')]
            if not csvs:
                return pd.DataFrame()
            with zf.open(csvs[0]) as f:
                for chunk in pd.read_csv(f, chunksize=chunksize):
                    recs = self.extract_relevant_records(chunk)
                    recs = self.assign_port_names(recs)
                    frames.append(self.identify_port_visit(recs))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def concat_all_zips(self, folder_path: str) -> pd.DataFrame:
        """Process all ZIPs in a folder and combine unique visits."""
        zip_files = sorted([
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if f.endswith('.zip')
        ])
        visits = [self.process_zip(zp) for zp in zip_files]
        if visits:
            df = pd.concat(visits, ignore_index=True)
            return df.drop_duplicates('MMSI', keep='first')
        return pd.DataFrame()

    def download_data(self, start_date: str, end_date: str, save_folder: str) -> None:
        """Download NOAA AIS daily ZIPs between two dates."""
        os.makedirs(save_folder, exist_ok=True)
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        for n in range((end - start).days + 1):
            dt = start + timedelta(days=n)
            fname = f"AIS_{dt.strftime('%Y_%m_%d')}.zip"
            url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2020/{fname}"
            out = os.path.join(save_folder, fname)
            if not os.path.exists(out):
                r = requests.get(url)
                if r.status_code == 200:
                    with open(out, 'wb') as f:
                        f.write(r.content)
                else:
                    print(f"Failed download {fname}: {r.status_code}")

    def delete_zips(self, folder_path: str) -> None:
        """Delete all ZIP files in the specified folder."""
        zip_files = [os.path.join(folder_path, f)
                     for f in os.listdir(folder_path) if f.endswith('.zip')]
        for zp in zip_files:
            try:
                os.remove(zp)
                print(f"Deleted {zp}")
            except Exception as e:
                print(f"Error deleting {zp}: {e}")