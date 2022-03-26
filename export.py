import os.path
from datetime import datetime

import pandas as pd

from mappings import map_kwargs
from mongo import EmercitMongo


def export_csv(from_date: datetime, to_date: datetime, data_type: str = "river_level"):
    em = EmercitMongo(host='192.168.5.203')

    for feature in em.get_features(data_type=data_type):
        ftr_id = feature.get("properties", {}).get("id", None)
        ftr_name = feature.get("properties", {}).get("name", None)
        print(ftr_name)
        csv_file_path = f"F:/export/{ftr_name}-{from_date.date()}-{to_date.date()}-{data_type}.csv"
        if os.path.exists(csv_file_path):
            print("File already exists")
            continue
        if not ftr_id or not ftr_name:
            print("Can't get feature id or name", feature)
            continue
        measurements = list(em.get_measurements(
            station_id=ftr_id,
            period_from=from_date,
            period_to=to_date,
            **map_kwargs(data_type)
        ))
        if not measurements:
            print("No measurements found")
            continue
        station_df = pd.DataFrame(measurements)
        station_df['name'] = ftr_name
        station_df.drop(columns=["station_id"], inplace=True)
        station_df.rename(columns={"time": "datetime"}, inplace=True)
        if data_type == "river_level":
            station_df.rename(columns={"d": "distance", "z": "zero"}, inplace=True)
            station_df = station_df[["name", "datetime", "bs", "distance", "zero"]]
        else:
            station_df = station_df[["name", "datetime", data_type]]
        station_df["datetime"] = station_df["datetime"].dt.tz_localize(None)
        print(station_df)
        station_df.to_csv(
            csv_file_path,
            sep=";",
            decimal=",",
            float_format="%.3f",
            index=False,
            date_format="%d.%m.%Y %H:%M:%S",
        )


if __name__ == '__main__':
    export_csv(from_date=datetime(year=2014, month=4, day=1), to_date=datetime(year=2021, month=3, day=26), data_type="river_level")
