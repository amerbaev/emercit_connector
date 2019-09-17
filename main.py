from datetime import datetime, timedelta
from pprint import pprint

from connector import EmercitConnector
from mongo import EmercitMongo


def main():
    ec = EmercitConnector()
    em = EmercitMongo()
    # result, _, _ = ec.mgraph(station_id=122,
    #                          mode='distance',
    #                          date_from=datetime.today().date() - timedelta(days=50),
    #                          date_to=datetime.today().date())
    # dates = [date.isoformat() for date in result['d'].keys()]
    # # print(dates)
    # em.save_measurements(station_id=122, mode='distance', measurements=result)
    pprint(list(em.get_measurements(station_id=122,
                                    mode='distance',
                                    period_from=datetime.combine(datetime.today().date(), datetime.min.time()),
                                    period_to=datetime.today().date() + timedelta(days=1))))


if __name__ == '__main__':
    main()
