import concurrent.futures
import logging
from datetime import date, timedelta
from typing import List, Tuple, Dict, Any

from tqdm import tqdm

from connector import EmercitConnector
from mappings import map_kwargs
from mongo import EmercitMongo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class EmercitProvider:
    def __init__(self, connector: EmercitConnector = None, mongo: EmercitMongo = None):
        if connector is None:
            connector = EmercitConnector()
        if mongo is None:
            mongo = EmercitMongo()

        self._connector = connector
        self._mongo = mongo

    def dump_all(self, from_date: date, to_date: date = date.today(), fields: list = None):
        logger.info('Getting all features')
        features = self._connector.overall()
        logger.info('Saving features')
        self._mongo.save_features(features)

        exists_modes = {}
        for future in features:
            properties = future['properties']
            feature_id = properties['id']
            for field, val in properties['data'].items():
                if fields is not None and field not in fields:
                    continue
                if val is not None:
                    if feature_id not in exists_modes:
                        exists_modes[feature_id] = []
                    exists_modes[feature_id].append(map_kwargs(field))

        days_delta = (to_date - from_date).days
        intervals: List[Tuple[date, date]] = []
        for delta in range(0, days_delta + 1, 1):
            interval_start = from_date + timedelta(days=delta)
            interval_end = from_date + timedelta(days=delta)
            if interval_end > to_date:
                interval_end = to_date
            intervals.append((interval_start, interval_end))

        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            interval_measurements = {executor.submit(self.get_measurements, start, end, exists_modes): (start, end)
                                     for start, end in intervals}
            with tqdm(total=len(interval_measurements)) as pbar:
                for future in concurrent.futures.as_completed(interval_measurements):
                    logger.debug("Next result is coming")
                    interval = interval_measurements[future]
                    try:
                        future.result()
                    except:
                        logger.debug("Can't get measurements for interval: ", str(interval[0]), "-", str(interval[1]))
                    finally:
                        pbar.update(1)

    def get_measurements(self, date_from, date_to, exists_modes) -> Dict[str, Dict[str, Any]]:
        measurements = {}
        for feature_id, modes in exists_modes.items():
            for kwargs in modes:
                try:
                    measure, _, _ = self._connector.mgraph(station_id=feature_id,
                                                           date_from=date_from,
                                                           date_to=date_to,
                                                           **kwargs)
                    if feature_id not in measurements:
                        measurements[feature_id] = {}
                    self._mongo.save_measurements(station_id=feature_id,
                                                  mode=kwargs['mode'],
                                                  measurements=measure)
                except Exception as e:
                    logger.warning("Can't get measurements for station {} from {} to {} with params {}"
                                   .format(feature_id, date_from, date_to, kwargs))
                    logger.error(e)
        return measurements


if __name__ == '__main__':
    mongo = EmercitMongo(host='192.168.5.203')
    EmercitProvider(mongo=mongo).dump_all(from_date=date(year=2014, month=1, day=1),
                                          fields=['river_level', 'temperature', 'humidity'])
