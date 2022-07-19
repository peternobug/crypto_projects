from pybit import usdt_perpetual
from datetime import datetime
from time import sleep
from typing import Union
from pprint import pprint
import pandas as pd
import random

class Bybit_usdt_perpetual_kline:
    def __init__(self, symbol: Union[int, list], interval: Union[int, list], start_date: str, end_date: str):
        """

        :param symbol:
        :param interval:
        :param start_date:
        :param end_date:
        """
        self.buffer = None
        self.http_receiver = None
        self.symbol = symbol if type(symbol) == list else [symbol]
        self.interval = interval if type(interval) == list else [interval]
        self.start_date = start_date
        self.end_date = end_date
        self.http_receiver = usdt_perpetual.HTTP()

    def get_symbol_list(self):
        """
        Retrieving a list of symbol that is traded in the asset_type
        :return:
        """

        http_response = self.http_receiver.query_symbol()
        pprint(http_response['result'])
        return http_response['result'] if 'result' in http_response else []

    def increment_generator(self, interval: str) -> int:
        """

        :param interval:
        :return:
        """
        self.buffer = 0.2
        character_map_minutes_dict = {
            'D': 24 * 60,
            'W': 24 * 60 * 7,
            'M': 24 * 60 * 7 * 28,
        }

        if interval.isalpha():
            interval_in_minutes = character_map_minutes_dict[interval]
        else:
            interval_in_minutes = int(interval)

        res = int(interval_in_minutes * 60 * 200 * (1 - self.buffer))
        return res, interval_in_minutes

    def date_string_to_timestamp(self):
        start_datetime = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(self.end_date, '%Y-%m-%d')

        start_timestamp = datetime.timestamp(start_datetime)
        end_timestamp = datetime.timestamp(end_datetime)

        start_timestamp -= 60 * 60 * 8
        end_timestamp -= 60 * 60 * 8

        return start_timestamp, end_timestamp

    def kLine_downloader(self, symbol: str, interval: str) -> list:
        """

        :param symbol:
        :param interval:
        :return:
        """
        res_list = []

        increment_timestamp, interval_in_minutes = self.increment_generator(interval)
        start_timestamp, end_timestamp = self.date_string_to_timestamp()

        temp_from_timestamp = start_timestamp
        while True:
            receiver_param = {
                "symbol": symbol.upper(),
                "interval": interval,
                "from": temp_from_timestamp,
                'limit': 200
            }
            if temp_from_timestamp + increment_timestamp > end_timestamp:
                receiver_param['limit'] = round((end_timestamp - temp_from_timestamp) / (60 * interval_in_minutes))

            try:
                http_response = self.http_receiver.query_kline(**receiver_param)
                # print(http_response)
                if 'result' in http_response.keys():
                    res_list += http_response['result']
                else:
                    raise ValueError(
                        f'Fail to request symbol:{symbol}, interval:{interval} KLine with Request Parameters: {receiver_param}')
            except Exception as e:
                print(e)
            temp_from_timestamp += increment_timestamp
            if temp_from_timestamp > end_timestamp:
                return res_list
            sleep(random.uniform(0.01, 0.05))

    def _kLine_handler(self, symbol: str, interval: str, data_list: list = []):
        """

        :param symbol:
        :param interval:
        :param data_list:
        :return:
        """
        df = pd.DataFrame(data_list)
        df.drop_duplicates(inplace=True)
        df.to_csv(f"bybit_{symbol}_{interval}_kline.csv")

    def _symbol_validator(self, symbol: str):
        """

        :param symbol:
        :return:
        """
        standard_interval_list = ['1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'M', 'W']
        if symbol not in standard_interval_list:
            raise ValueError(f"Interval not validated, please input one or more than one of the {standard_interval_list}")

    def run(self):
        """

        :return:
        """
        for sym in self.symbol:
            for interval in self.interval:
                self._symbol_validator(interval)
                try:
                    print(sym, interval)
                    kLine_list = self.kLine_downloader(symbol=sym, interval=interval)
                    if kLine_list:
                        self._kLine_handler(symbol=sym.upper(), interval=interval, data_list=kLine_list)
                    else:
                        raise ValueError("No data was crawled, pleaase check again with your parameters!")
                except Exception as e:
                    print(e)


if __name__ == "__main__":
    start_date = "2021-06-24"
    end_date = "2022-06-25"
    symbol = "SANDUSDT"
    interval = "30"
    bybit = Bybit_usdt_perpetual_kline(symbol=symbol, interval=interval, start_date=start_date, end_date=end_date)
    bybit.run()
    # historical_data(symbol=symbol, interval=interval, start_date=start_date, end_date=end_date).get_symbol_list()
