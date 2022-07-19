"""
This is about polygon.io API code
"""
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

pd.set_option('display.max_columns', 500)


class PolygonClient:
    def __init__(self):
        # TODO : Please input your own Glassnode api key
        self.api_key = ""

    def _get_data(self, url: str):
        try:
            r = requests.get(url=url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)
        print(r.text)

        if "open-close" in url:
            res_dict = json.loads(r.text)['openTrades']
        else:
            res_dict = json.loads(r.text)['results']
        res_df = pd.DataFrame(res_dict)
        return res_df

    def get_aggregates_bars(self, symbol: str, timeframe: str, from_time: str, to_time: str, adjusted: bool = True,
                            asc: bool = True):
        """
        /v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}
        Get aggregate bars for a cryptocurrency pair over a given date range in custom time window sizes.
        For example, if timespan = ‘minute’ and multiplier = ‘5’ then 5-minute bars will be returned.
        url: https://polygon.io/docs/crypto/get_v2_aggs_ticker__cryptoticker__range__multiplier___timespan___from___to
        :param symbol: The ticker symbol of the currency pair.
        :param timeframe: The size of the time window.
        :param from_time: The start of the aggregate time window.
        Either a date with the format YYYY-MM-DD or a millisecond timestamp.
        :param to_time: The end of the aggregate time window.
        Either a date with the format YYYY-MM-DD or a millisecond timestamp.
        :param adjusted: Whether or not the results are adjusted for splits. By default, results are adjusted.
        Set this to false to get results that are NOT adjusted for splits.
        :param asc: Sort the results by timestamp.
        True will return results in ascending order (oldest at the top),
        False will return results in descending order (newest at the top).
        :return: Requested aggregates bars dataframe
        """
        date_string_format = "%Y-%m-%d"
        to_datetime = datetime.strptime(to_time, date_string_format)
        one_month_before = datetime.now() - timedelta(3)
        one_month_before_string = one_month_before.strftime(date_string_format)
        if to_datetime > one_month_before:
            raise ValueError(
                f"This is free version, you can't get the latest data, please set to_date before {one_month_before_string}")
        adjusted = "false" if adjusted is False else "true"
        asc = "asc" if asc is True else "desc"
        url = f'https://api.polygon.io/v2/aggs/ticker/X:{symbol}/range/1/{timeframe}/{from_time}/{to_time}?' \
              f'adjusted={adjusted}&sort={asc}&limit=50000&apiKey={self.api_key}'
        df = self._get_data(url=url)
        df.rename(columns={'t': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume',
                           'vw': 'weighted_volume', 'n': 'number_of_transaction'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df[['datetime', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'weighted_volume',
                 'number_of_transaction']]
        df.dropna(inplace=True)
        df.reset_index(inplace=True, drop=True)
        return df

    def grouped_daily_bars(self, from_time: str, symbol_list: list = [], adjusted: bool = True):
        """
        /v2/aggs/grouped/locale/global/market/crypto/{date}
        Get the daily open, high, low, and close (OHLC) for the entire cryptocurrency markets.
        url: https://polygon.io/docs/crypto/get_v2_aggs_ticker__cryptoticker__range__multiplier___timespan___from___to
        :param from_time: The beginning date for the aggregate window.
        :param adjusted: Whether or not the results are adjusted for splits.
        By default, results are adjusted. Set this to false to get results that are NOT adjusted for splits.
        :return:
        """
        adjusted = "false" if adjusted == False else "true"
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{from_time}?" \
              f"adjusted={adjusted}&apiKey={self.api_key}"
        df = self._get_data(url=url)
        df.rename(columns={'T': 'symbol', 't': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c':
            'close', 'v': 'volume', 'vw': 'weighted_volume', 'n': 'number_of_transaction'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df[['symbol', 'datetime', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                 'weighted_volume', 'number_of_transaction']]
        if symbol_list:
            df = df.loc[df['symbol'].isin(['X:' + symbol for symbol in symbol_list])]
        df.dropna(inplace=True)
        df.reset_index(inplace=True, drop=True)
        return df

    def daily_open_close(self, symbol: str, from_time: str, adjusted: bool = True):
        """
        /v1/open-close/crypto/{from}/{to}/{date}
        Get the open, close prices of a cryptocurrency symbol on a certain day.
        :param symbol: The "from" symbol of the pair., The "to" symbol of the pair.
        :param from_time: The date of the requested open/close in the format YYYY-MM-DD.
        :param adjusted: Whether or not the results are adjusted for splits. By default, results are adjusted.
        Set this to false to get results that are NOT adjusted for splits.
        :return:
        """
        adjusted = "false" if adjusted is False else "true"
        url = f"https://api.polygon.io/v1/open-close/crypto/{symbol[:3]}/{symbol[-3:]}/{from_time}?" \
              f"adjusted={adjusted}&apiKey={self.api_key}"
        df = self._get_data(url=url)
        df = df[['t', 'p', 's', 'x']]
        df.rename(
            columns={'x': 'exchange', 'p': 'price', 's': 'size', 't': 'timestamp'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.dropna(inplace=True)
        df.reset_index(inplace=True, drop=True)
        return df

    def previous_close(self, symbol: str, adjusted: bool = True):
        """
        /v2/aggs/ticker/{cryptoTicker}/prev
        Get the previous day's open, high, low, and close (OHLC) for the specified cryptocurrency pair.
        :param symbol: The ticker symbol of the currency pair.
        :param adjusted: Whether or not the results are adjusted for splits. By default, results are adjusted.
        Set this to false to get results that are NOT adjusted for splits.
        :return:
        """
        adjusted = "false" if adjusted is False else "true"
        url = f"https://api.polygon.io/v2/aggs/ticker/X:{symbol}/prev?adjusted={adjusted}&apiKey={self.api_key}"
        df = self._get_data(url=url)
        df.rename(columns={'T': 'symbol', 't': 'timestamp', 'o': 'open', 'h': 'high', 'l': 'low', 'c':
            'close', 'v': 'volume', 'vw': 'weighted_volume', 'n': 'number_of_transaction'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df[['symbol', 'datetime', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                 'weighted_volume', 'number_of_transaction']]
        return df


if __name__ == "__main__":
    polygon = PolygonClient()
    polygon.get_aggregates_bars(symbol="BTCUSD", timeframe="day", from_time="2022-06-01", to_time="2022-06-10")
    # print(polygon.grouped_daily_bars(from_time="2022-05-02", symbol_list=['BTCUSD', 'ETHUSD']))
    # polygon.daily_open_close(symbol="BTCUSD", from_time="2022-05-01")
    # polygon.previous_close(symbol="BTCUSD")
