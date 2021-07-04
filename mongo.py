from datetime import datetime, date
from pprint import pprint
from typing import Dict, Union

import pytz
from bson import CodecOptions
from pymongo import MongoClient, UpdateOne
from retrying import retry


class EmercitMongo:
    _timezone = pytz.timezone('Etc/GMT-3')
    _codec_options = CodecOptions(tz_aware=True, tzinfo=_timezone)

    def __init__(self, host: str = '192.168.5.203', port: int = 27017, dbname: str = 'emercit'):
        mgo = MongoClient(host=host, port=port)
        self._db = mgo[dbname]

    @retry
    def save_features(self, features: list):
        features_col = self._db['features']
        for feature in features:
            feature_filter = {'properties.id': feature['properties']['id']}
            feature_update = {'$set': feature}
            features_col.update_one(feature_filter, feature_update, upsert=True)

    @retry
    def save_measurements(self, station_id: int, mode: str,
                          measurements: Dict[str, Dict[datetime, Union[None, int, float, str]]]):
        mes_updates = {}
        for mes_type, mes_ts in measurements.items():
            for mes_time, mes_val in mes_ts.items():
                if mes_time not in mes_updates:
                    mes_updates[mes_time] = {
                        'station_id': station_id,
                        'mode': mode,
                        'time': mes_time,
                    }
                mes_updates[mes_time][mes_type] = mes_val

        bulk_updates = [
            UpdateOne({
                'station_id': update['station_id'],
                'mode': update['mode'],
                'time': update['time']
            }, {'$set': update}, upsert=True)
            for update in mes_updates.values()
        ]

        if not bulk_updates:
            return

        measurements_col = self._db['measurements'].with_options(codec_options=self._codec_options)
        measurements_col.bulk_write(bulk_updates, ordered=False)

    @retry
    def get_measurements(self, station_id: int, mode: str,
                         period_from: Union[datetime, date],
                         period_to: [datetime, date],
                         timezone=None):
        if timezone is None:
            timezone = self._timezone

        if isinstance(period_from, date):
            period_from = datetime.combine(period_from, datetime.min.time(), tzinfo=timezone)
        if isinstance(period_to, date):
            period_to = datetime.combine(period_to, datetime.min.time(), tzinfo=timezone)

        measurements_col = self._db['measurements'].with_options(codec_options=self._codec_options)
        mes_cur = measurements_col.find({
            'station_id': station_id,
            'mode': mode,
            'time': {
                '$gte': period_from,
                '$lt': period_to
            }
        }, {'_id': 0, 'mode': 0}, sort=[('time', 1)])
        for mes in mes_cur:
            yield mes

    @retry
    def get_features(self, data_type: str = "river_level"):
        features_col = self._db['features'].with_options(codec_options=self._codec_options)
        ftr_cur = features_col.find({
            f'properties.data.{data_type}': {'$ne': None},
        }, sort=[("properties.name", 1)], no_cursor_timeout=True)
        for ftr in ftr_cur:
            yield ftr

    @retry
    def get_station(self, station_id: int):
        stations_col = self._db["stations"]
        station = stations_col.find_one({"id": station_id})
        return station


if __name__ == '__main__':
    em = EmercitMongo()
    pprint(list(em.get_features()))
