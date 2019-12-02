from datetime import date, timedelta

from connector import EmercitConnector
from mongo import EmercitMongo
from mappings import map_kwargs

from tqdm import tqdm


class EmercitProvider:
    def __init__(self, connector: EmercitConnector = None, mongo: EmercitMongo = None):
        if connector is None:
            connector = EmercitConnector()
        if mongo is None:
            mongo = EmercitMongo()

        self._connector = connector
        self._mongo = mongo

    def dump_all(self, from_date: date, to_date: date = date.today(), fields: list = None):
        print('Getting all features')
        features = self._connector.overall()
        print('Saving features')
        self._mongo.save_features(features)

        exists_modes = {}
        for feature in features:
            properties = feature['properties']
            feature_id = properties['id']
            for field, val in properties['data'].items():
                if fields is not None and field not in fields:
                    continue
                if val is not None:
                    if feature_id not in exists_modes:
                        exists_modes[feature_id] = []
                    exists_modes[feature_id].append(map_kwargs(field))

        days_delta = (to_date - from_date).days
        for delta in tqdm(range(0, days_delta + 1, 50)):
            date_from = from_date + timedelta(days=delta)
            date_to = from_date + timedelta(days=delta + 49)
            for feature_id, modes in tqdm(exists_modes.items()):
                for kwargs in modes:
                    try:
                        measurements, _, _ = self._connector.mgraph(station_id=feature_id,
                                                                    date_from=date_from,
                                                                    date_to=date_to,
                                                                    **kwargs)
                    except Exception as e:
                        print("Can't get measurements for station {} from {} to {} with params {}"
                              .format(feature_id, date_from, date_to, kwargs))
                        print(e)
                    else:
                        self._mongo.save_measurements(station_id=feature_id,
                                                      mode=kwargs['mode'],
                                                      measurements=measurements)

if __name__ == '__main__':
    mongo = EmercitMongo(host='10.101.101.2')
    EmercitProvider(mongo=mongo).dump_all(from_date=date(year=2019, month=1, day=1),
                                          fields=['river_level', 'temperature', 'humidity'])
