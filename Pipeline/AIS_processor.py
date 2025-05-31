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

    def extract_first_arrivals_anywhere(self,df):
        # Filter by relevant vessel types
        df = df[
            df["VesselType"].isin(range(70, 90)) | df["VesselType"].isin([30, 52])
        ].copy()

        # Drop rows with missing or invalid coordinates
        df = df.dropna(subset=["LAT", "LON"])
        df = df[(df["LAT"] != 0) & (df["LON"] != 0)]

        # Convert timestamps
        df["BaseDateTime"] = pd.to_datetime(df["BaseDateTime"], errors='coerce')

        df = df.dropna(subset=["BaseDateTime"])

        # Filter for ships that are of Status 1 & 5 (Anchored)
        if "Status" in df.columns:
            df = df[df["Status"].isin([1, 5])]

        """
        Working on cleaning this section
        """
        # Drop all the columns that are not need
        columns_to_drop = ["SOG", "COG", "Heading", "IMO", "VesselName", "Length", "Width", "TransceiverClass", "Cargo", "CallSign", "Draft"]
        existing_cols = [col for col in columns_to_drop if col in df.columns]

        df = df.drop(columns=existing_cols)

        # End of cleaning

        # Sort and get the first ping per MMSI
        first_arrivals = (
            df.sort_values(["MMSI", "BaseDateTime"])
            .drop_duplicates("MMSI", keep="first")
        )
        
        return self.assign_port_names(first_arrivals)
    
    def process_zip_in_chunks(self,zip_path, chunksize=100_000):

        all_cleaned_chunks = []
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
            if not csv_files:
                return pd.DataFrame()

            with zip_ref.open(csv_files[0]) as f:
                reader = pd.read_csv(f, chunksize=chunksize)
                for chunk in reader:
                    cleaned = self.extract_first_arrivals_anywhere(chunk)
                    all_cleaned_chunks.append(cleaned)

        return pd.concat(all_cleaned_chunks, ignore_index=True)
    
    def concat_all_zips(self, folder_path: str) -> pd.DataFrame:
        """Process all ZIPs in a folder and combine unique visits."""
        zip_files = sorted([
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if f.endswith('.zip')
        ])
        visits = [self.process_zip_in_chunks(zp) for zp in zip_files]
        if visits:
            df = pd.concat(visits, ignore_index=True)
            return df.drop_duplicates('MMSI', keep='first')
        return pd.DataFrame()

    def download_ais_data(self, start_date_str, end_date_str, save_folder):
        os.makedirs(save_folder, exist_ok=True)
        print(f"Files will be saved to: {save_folder}")

        # Convert string dates to datetime objects
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")


        # Download each file in the date range
        for i in range((end_date - start_date).days + 1):
            date_obj = start_date + timedelta(days=i)
            filename = f"AIS_{date_obj.strftime('%Y_%m_%d')}.zip"
            url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2020/{filename}"
            file_path = os.path.join(save_folder, filename)

            if not os.path.exists(file_path):
                print(f"Downloading {filename}...")
                response = requests.get(url)
                if response.status_code == 200:
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                    print(f"Saved: {file_path}")
                else:
                    print(f"Failed: {response.status_code}")
            else:
                print(f"Already downloaded: {filename}")

    def clean_and_save_first_arrivals(self, vessel_data, output_csv_path):
        # Drop rows where 'Port Name' is 'Unknown'
        cleaned_df = vessel_data[vessel_data['Port Name'] != 'Unknown'].copy()

        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

        # Save the cleaned DataFrame to a CSV file
        cleaned_df.to_csv(output_csv_path, index=False)

        print(f"Cleaned data saved to: {output_csv_path}")
        print("Shape of the cleaned data:", cleaned_df.shape)

        return cleaned_df

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

    def run(self, start_date: str, end_date: str, save_folder: str, output_csv_path: str) -> pd.DataFrame:
        """Main method to download, process, and save AIS port visit data."""

        # Downloading all the data at once isn't feasible due to size. Let's break down to weekly iterations.
        
        #1 Calculate the number of weeks between start and end dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        if start.year != end.year:
            raise ValueError("Start and end dates must be in the same year.")
        num_days = (end - start).days + 1
        num_weeks = (num_days // 7) + (1 if num_days % 7 > 0 else 0)
        print(f"Number of weeks to process: {num_weeks}")
        
        #2 Iterate over weeks and download data
        agg_cleaned_visits = pd.DataFrame()
        for week in range(num_weeks):
            week_start = start + timedelta(weeks=week)
            week_end = min(end, week_start + timedelta(days=6))
            print(f"Processing week {week + 1}/{num_weeks}: {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")
            self.download_ais_data(week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d"), save_folder)
            visits = self.concat_all_zips(save_folder)
            visits = self.clean_and_save_first_arrivals(visits, output_csv_path)
            agg_cleaned_visits = pd.concat([agg_cleaned_visits, visits], ignore_index=True)
            # Delete the downloaded zips after processing each week
            self.delete_zips(save_folder)
        
        #3 Return the aggregated cleaned visits
        if agg_cleaned_visits.empty:
            print("No visits found in the specified date range.")
            return pd.DataFrame()
        print(f"Total unique visits found: {len(agg_cleaned_visits)}")
        return agg_cleaned_visits