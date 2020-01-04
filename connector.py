import time
import datetime
from typing import Union

import requests
import pytz
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class EmercitConnector:
    BASE_URL = 'http://emercit.com/map'

    def __init__(self):
        self.features = []
        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])
        self.session = requests.session()
        self.session.mount('http://', HTTPAdapter(max_retries=retries, pool_maxsize=100))

    def overall(self, unix_time: int = None) -> list:
        if unix_time is None:
            unix_time = int(time.time())

        overall_url = '/'.join([self.BASE_URL, f'overall.php?time={unix_time}'])

        overall_response = self.session.get(overall_url)
        if overall_response.status_code != 200:
            raise RuntimeError('Overall status code: %s', overall_response.status_code)

        overall_json: dict = overall_response.json()
        features: list = overall_json['features']
        if not features:
            raise RuntimeError('No features returned')
        self.features = features
        return features

    def mgraph(self, station_id: int = None, station_name: str = None,
               date_from: Union[str, datetime.date] = None, date_to: Union[str, datetime.date] = None,
               mode: str = 'distance', nocache: int = None, **kwargs):
        if nocache is None:
            nocache = int(time.time())

        if station_id is None and station_name is None:
            raise RuntimeError('No station id or name provided')
        elif station_id is None and station_name is not None:
            station_id = self._get_feature_by_name(station_name)['properties']['id']

        today_date = datetime.date.today()
        if date_to is None:
            date_to = today_date if date_from is None else datetime.datetime.strptime(date_from, '%Y-%m-%d').date() if isinstance(date_from, str) else date_from
        if date_from is None:
            date_from = today_date if today_date <= date_to else date_to

        if isinstance(date_from, (datetime.date, datetime.datetime)):
            date_from = date_from.strftime('%Y-%m-%d')
        if isinstance(date_to, (datetime.date, datetime.datetime)):
            date_to = date_to.strftime('%Y-%m-%d')

        mgraph_params = {
            'mode': mode,
            'id': station_id,
            'a': date_from,
            'b': date_to,
            'nocache': nocache,
            **kwargs
        }

        mgraph_substring = '&'.join([f'{p_key}={p_value}' for p_key, p_value in mgraph_params.items()])

        mgraph_url = '/'.join([self.BASE_URL, f'mgraph.php?{mgraph_substring}'])

        try:
            mgraph_response = self.session.get(mgraph_url)
        except requests.exceptions.RetryError:
            raise RuntimeError('')

        if mgraph_response.status_code != 200:
            raise RuntimeError('MGraph status code: %s', mgraph_response.status_code)

        mgraph_json: dict = mgraph_response.json()
        period_from = datetime.datetime.fromisoformat(mgraph_json.pop('period_1'))
        period_to = datetime.datetime.fromisoformat(mgraph_json.pop('period_2'))

        observations = {}
        for obs_key, obs_values in mgraph_json.items():
            if obs_values is None:
                continue
            observations[obs_key] = {
                # yes GMT-(MIUNIS)3 because fuck you
                datetime.datetime.fromtimestamp(obs[0] // 1000, tz=pytz.timezone('Etc/GMT-3')): obs[1]
                for obs in obs_values if obs[1] is not None
            }

        return observations, period_from, period_to

    def _get_feature_by_name(self, station_name: str) -> dict:
        if not self.features:
            self.overall()
        feature = next(feature
                       for feature in self.features
                       if feature.get('properties', {}).get('name', '').lower() == station_name.lower())
        return feature


if __name__ == '__main__':
    emercit_connector = EmercitConnector()
    observations, _, _ = emercit_connector.mgraph(station_name='АГК-0058', date_from='2016-01-01')
