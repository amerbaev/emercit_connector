from datetime import date, timedelta

from connector import EmercitConnector
from mongo import EmercitMongo
from mappings import map_kwargs


class EmercitProvider:
    def __init__(self, connector: EmercitConnector = None, mongo: EmercitMongo = None):
        if connector is None:
            connector = EmercitConnector()
        if mongo is None:
            mongo = EmercitMongo()

        self._connector = connector
        self._mongo = mongo

    def dump_all(self, from_date: date, to_date: date = date.today()):
        features = self._connector.overall()
        self._mongo.save_features(features)

        exists_modes = {}
        for feature in features:
            properties = feature['properties']
            for field, val in properties['data'].items():
                if val is not None:
                    feature_id = properties['id']
                    if feature_id not in exists_modes:
                        exists_modes[feature_id] = []
                    exists_modes[feature_id].append(map_kwargs(field))

        days_delta = (to_date - from_date).days
        for delta in range(0, days_delta + 1, 50):
            date_from = from_date + timedelta(days=delta)
            date_to = from_date + timedelta(days=delta + 49)
            for feature_id, modes in exists_modes.items():
                for kwargs in modes:
                    measurements, _, _ = self._connector.mgraph(station_id=feature_id,
                                                                date_from=date_from,
                                                                date_to=date_to,
                                                                **kwargs)
                    self._mongo.save_measurements(station_id=feature_id,
                                                  mode=kwargs['mode'],
                                                  measurements=measurements)


if __name__ == '__main__':
    EmercitProvider().dump_all(from_date=date(year=2019, month=1, day=1))
