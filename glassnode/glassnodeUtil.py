import requests
import pandas as pd
import json
from datetime import datetime
import time
import random
from requests.exceptions import HTTPError

class glassnodeConsumer:
    def __init__(self):
        self.base_url = 'https://api.glassnode.com/'
        # TODO : Please input your own Glassnode api key
        self.api_key = ''

    def get_api_request(self, from_date: str, to_date: str, symbol:str, category: str, interval: str, topic: str) -> pd.DataFrame:
        if not symbol:
            raise Exception('symbol can not be empty')
        try:
            from_date = glassnodeConsumer.string_to_unix_timestamp(from_date)
            to_date   = glassnodeConsumer.string_to_unix_timestamp(to_date)
        except ValueError as err:
            if from_date == '' or to_date == '':
                pass
            else:
                print(f'The format of the date string should follow YYYY-MM-DD : {err}')
        try:
            x = requests.get(url=f'{self.base_url}v1/metrics/{category}/{topic}', params={'a':symbol,'api_key':self.api_key,'s':from_date, 'u':to_date,'i':interval, 'f':'JSON'})
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        if x.status_code == 400:
            raise RuntimeError(f'status code: 400, error occurred')
        elif x.status_code == 404:
            raise ValueError(f'status code: 404, error occurred')
        raw_text = x.text
        print(raw_text)
        if not json.loads(raw_text) :
            raise TypeError('getting a empty list, something went wrong, please check your input arguments!')
        time.sleep(random.random())
        return raw_text

    def get_close_price(self, symbol:str = '', from_date: str = '', to_date: str = '', interval: str = '') -> pd.DataFrame:
        """
        Describe: The asset's closing price in USD.
        :param symbol: crypto symbol
        :param from date: start date
        :param to date: end date
        :param interval: timeframe
        :return: pd.DataFrame
        """
        if symbol not in glassnodeConsumer.supported_type():
            raise ValueError(f'symbol is wrong, symbol should be one of {glassnodeConsumer.supported_type()}')
        if interval not in glassnodeConsumer.suppoerted_interval():
            raise ValueError(
                f'interval is wrong, symbol should be one of {glassnodeConsumer.suppoerted_interval()[1:]}')
        json_object = self.get_api_request(from_date = from_date, to_date = to_date, symbol= symbol, category= 'market', interval = interval, topic= 'price_usd_close')
        df = pd.DataFrame(json.loads(json_object))
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.columns = ['datetime', 'close_price']
        return df

    def get_ohlc(self, symbol:str = '', from_date: str = '', to_date: str = '', interval: str = '') -> pd.DataFrame:
        """
        Describe: OHLC candlestick chart of the asset's price in USD.
        :param symbol: crypto symbol
        :param from date: start date
        :param to date: end date
        :param interval: timeframe
        :return: pd.DataFrame
        """
        if symbol not in glassnodeConsumer.supported_type():
            raise ValueError(f'symbol is wrong, symbol should be one of {glassnodeConsumer.supported_type()}')
        if interval not in glassnodeConsumer.suppoerted_interval()[:3]:
            raise ValueError(
                f'interval is wrong, symbol should be one of {glassnodeConsumer.suppoerted_interval()[:3]}')
        json_object = self.get_api_request(from_date = from_date, to_date = to_date, symbol= symbol, category= 'market', interval = interval, topic= 'price_usd_ohlc')
        df = pd.DataFrame([{'t':object['t']}|object['o'] for object in json.loads(json_object)])
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.columns = ['datetime', 'close', 'high', 'low', 'open']
        df = df[['datetime', 'open', 'high', 'low', 'close']]
        return df

    def get_issuance(self, symbol: str = '', from_date: str = '', to_date: str = '',
                        interval: str = '') -> pd.DataFrame:
        """
        Describe: The total amount of new coins added to the current supply, i.e. minted coins or new coins released to the network.
        :param symbol: crypto symbol
        :param from date: start date
        :param to date: end date
        :param interval: timeframe
        :return: pd.DataFrame
        """
        symbol_constrain_list = ['BTC', 'ETH']
        if symbol not in symbol_constrain_list:
            raise ValueError(f'symbol is wrong, symbol should be one of {symbol_constrain_list}')
        if interval not in glassnodeConsumer.suppoerted_interval()[:3]:
            raise ValueError(
                f'interval is wrong, symbol should be one of {glassnodeConsumer.suppoerted_interval()[1:3]}')
        json_object = self.get_api_request(from_date=from_date, to_date=to_date, symbol=symbol, category='supply',
                                           interval=interval, topic='issued')
        df = pd.DataFrame(json.loads(json_object))
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.columns = ['datetime', 'issued']
        return df

    def get_inflation_rate(self, symbol: str = '', from_date: str = '', to_date: str = '',
                        interval: str = '') -> pd.DataFrame:
        """
        Describe: The yearly inflation rate, i.e. the percentage of new coins issued, divided by the current supply (annualized).
        :param symbol: crypto symbol
        :param from date: start date
        :param to date: end date
        :param interval: timeframe
        :return: pd.DataFrame
        """
        symbol_constrain_list = ['BTC', 'ETH']
        if symbol not in symbol_constrain_list:
            raise ValueError(f'symbol is wrong, symbol should be one of {symbol_constrain_list}')
        if interval not in glassnodeConsumer.suppoerted_interval()[:3]:
            raise ValueError(
                f'interval is wrong, symbol should be one of {glassnodeConsumer.suppoerted_interval()[1:3]}')
        json_object = self.get_api_request(from_date=from_date, to_date=to_date, symbol=symbol, category='supply',
                                           interval=interval, topic='inflation_rate')
        df = pd.DataFrame(json.loads(json_object))
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.columns = ['datetime', 'inflation_rate']
        return df

    def get_puell_multiple(self, symbol: str = '', from_date: str = '', to_date: str = '',
                        interval: str = '') -> pd.DataFrame:
        """
        Describe: The Puell Multiple is calculated by dividing the daily issuance value of bitcoins (in USD) by the 365-day moving average of daily issuance value.
        This metric was created by David Puell.
        For a detailed description see this article by @cryptopoiesis (https://twitter.com/cryptopoiesis).
        :param symbol: crypto symbol
        :param from date: start date
        :param to date: end date
        :param interval: timeframe
        :return: pd.DataFrame
        """
        symbol_constrain_list = ['BTC', 'LTC']
        if symbol not in symbol_constrain_list:
            raise ValueError(f'symbol is wrong, symbol should be one of {symbol_constrain_list}')
        if interval not in glassnodeConsumer.suppoerted_interval()[:2]:
            raise ValueError(
                f'interval is wrong, symbol should be one of {glassnodeConsumer.suppoerted_interval()[1:2]}')
        json_object = self.get_api_request(from_date=from_date, to_date=to_date, symbol=symbol, category='indicators',
                                           interval=interval, topic='puell_multiple')
        df = pd.DataFrame(json.loads(json_object))
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.columns = ['datetime', 'puell_multiple']
        return df

    def get_total_fees(self, symbol: str = '', from_date: str = '', to_date: str = '',
                        interval: str = '') -> pd.DataFrame:
        """
        Describe: The total amount of fees paid to miners. Issued (minted) coins are not included.
        :param symbol: crypto symbol
        :param from date: start date
        :param to date: end date
        :param interval: timeframe
        :return: pd.DataFrame
        """
        symbol_constrain_list = ['BTC','ETH', 'LTC']
        if symbol not in symbol_constrain_list:
            raise ValueError(f'symbol is wrong, symbol should be one of {symbol_constrain_list}')
        if interval not in glassnodeConsumer.suppoerted_interval():
            raise ValueError(
                f'interval is wrong, symbol should be one of {glassnodeConsumer.suppoerted_interval()}')
        json_object = self.get_api_request(from_date=from_date, to_date=to_date, symbol=symbol, category='fees',
                                           interval=interval, topic='volume_sum')
        df = pd.DataFrame(json.loads(json_object))
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.columns = ['datetime', 'total_fees']
        return df

    @staticmethod
    def string_to_unix_timestamp(date_string: str) -> float:
        datetime_object = datetime.strptime(date_string, '%Y-%m-%d')
        res_timestamp = datetime.timestamp(datetime_object)
        return int(res_timestamp)

    @staticmethod
    def supported_type():
        supported_list = ["BTC", "ETH", "LTC", "AAVE", "ABT", "AMPL", "ANT", "ARMOR", "BADGER", "BAL",
                          "BAND", "BAT", "BIX", "BNT", "BOND", "BRD", "BUSD", "BZRX", "CELR", "CHSB",
                          "CND", "COMP", "CREAM", "CRO", "CRV", "CVC", "CVP", "DAI", "DDX", "DENT", "DGX",
                          "DHT", "DMG", "DODO", "DOUGH", "DRGN", "ELF", "ENG", "ENJ", "EURS", "FET", "FTT",
                          "FUN", "GNO", "GUSD", "HEGIC", "HOT", "HPT", "HT", "HUSD", "INDEX", "KCS", "LAMB",
                          "LBA", "LDO", "LEO", "LINK", "LOOM", "LRC", "MANA", "MATIC", "MCB", "MCO", "MFT",
                          "MIR", "MKR", "MLN", "MTA", "MTL", "MX", "NDX", "NEXO", "NFTX", "NMR", "Nsure",
                          "OCEAN", "OKB", "OMG", "PAY", "PERP", "PICKLE", "PNK", "PNT", "POLY", "POWR", "PPT",
                          "QASH", "QKC", "QNT", "RDN", "REN", "REP", "RLC", "ROOK", "RPL", "RSR", "SAI", "SAN",
                          "SNT", "SNX", "STAKE", "STORJ", "sUSD", "SUSHI", "TEL", "TOP", "UBT", "UMA", "UNI",
                          "USDC", "USDK", "USDP", "USDT", "UTK", "VERI", "WaBi", "WAX", "WBTC", "WETH", "wNXM",
                          "WTC", "YAM", "YFI", "ZRX"]
        return supported_list

    @staticmethod
    def suppoerted_interval():
        supported_list = ['','24h', '1h', '10m', '1w', '1month']
        return supported_list

if __name__ == "__main__":
    # df_dict = {}
    # df = get_glassnode_data(topic='supply/issued', symbol='BTC')
    # df_dict['train'] = df.loc[(df['datetime'] >= '2012-11-29') & (df['datetime'] <= '2016-07-09')]
    # df_dict['validate'] = df.loc[(df['datetime'] > '2016-07-09') & (df['datetime'] <= '2020-05-11')]
    # df_dict['test'] = df.loc[(df['datetime'] > '2020-05-11')]
    # basic_plot(df_dict['train'])
    glassnode_comsumer = glassnodeConsumer()
    # glassnode_comsumer.get_close_price(symbol='BTC', from_date='2020-01-01', to_date='2021-01-01', interval = '42')
    # glassnode_comsumer.get_ohlc(symbol='BTC', from_date='2020-01-01', to_date='2021-01-01', interval = '24h')
    # glassnode_comsumer.get_issuance(symbol='BUSD', from_date='2020-01-01', to_date='2021-01-01', interval = '24h')
    # glassnode_comsumer.get_inflation_rate(symbol='BTC', from_date='2020-01-01', to_date='2021-01-01', interval =
    # '24h') glassnode_comsumer.get_puell_multiple(symbol='BTC', from_date='2020-01-01', to_date='2021-01-01',
    # interval = '24h')
    glassnode_comsumer.get_total_fees(symbol='BTC', from_date='2020-01-01', to_date='2020-03-01', interval = '24h')