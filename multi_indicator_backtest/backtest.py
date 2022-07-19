import time
import pandas as pd
import math
from pprint import pprint
import matplotlib.pyplot as plt
from alpha import Alpha

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)


class Backtest:
    def __init__(self, para_dict: dict):
        """

        :param para_dict:
        """
        self.df = pd.DataFrame()
        self.initial_capital = 10000.0
        self.strategy_name = para_dict["strategy_name"]
        self.reference_window = para_dict["reference_window"]
        self.first_window = para_dict["first_window"]
        self.second_window = para_dict["second_window"]
        self.sub_window_multiplier = para_dict["sub_window_multiplier"]
        self.symbol = para_dict["symbol"]
        self.stop_loss = para_dict["stop_loss"]
        self.backtest_year = para_dict["backtest_year"]
        self.start_time = para_dict["start_time"]
        self.end_time = para_dict["end_time"]
        self.taker_fee = 0.000
        self.total_equity = self.initial_capital

    def get_mongo_kline(self, interval: str):
        """
        :param interval:
        """
        current_timestamp = time.time()
        start_from = time.time() - 60 * 60 * 24 * 365 * self.backtest_year
        # TODO : GET DATA FROM YOUR OWN SOURCE, AND SAVE EACH SYMBOL TO df_dict[sym] correspondingly
        df = pd.DataFrame()
        # Stupid way to convert timestamp to local timestamp
        df['timestamp'] += 60 * 60 * 8
        df["date_time"] = pd.to_datetime(df['timestamp'], unit='s')
        df = df[["date_time", "open", "high", "low", "close"]]
        df.sort_index(inplace=True)
        self.df = Alpha(df).add_indicator(strategy_name=self.strategy_name, reference_window=self.reference_window,
                                          sub_window_multiplier=self.sub_window_multiplier)

    def signal_generator(self, i: int):
        """

        :param i:
        :return:
        """
        williamsR = self.df.loc[i, self.strategy_name.lower().split('_')[0]]
        ma = self.df.loc[i, self.strategy_name.lower().split('_')[1]]
        now_close = self.df.loc[i, "close"]
        open_long_signal = williamsR < self.first_window and now_close > ma
        open_short_signal = williamsR > self.second_window and now_close < ma
        close_long_signal = williamsR > self.second_window
        close_short_signal = williamsR < self.first_window
        return open_long_signal, open_short_signal, close_long_signal, close_short_signal

    def first_trade_of_the_day(self, i: int):
        """

        :param i:
        :return:
        """
        williamsR = self.df.loc[i, self.strategy_name.lower().split('_')[0]]
        open_long_signal = williamsR < -80
        open_short_signal = williamsR > -20
        return open_long_signal, open_short_signal

    def check_stop_loss(self, close_long_signal: bool, close_short_signal: bool, holding_position: str,
                        open_price: float, now_close: float):
        """

        :param close_long_signal:
        :param close_short_signal:
        :param holding_position:
        :param open_price:
        :param now_close:
        :return:
        """
        if holding_position == "LONG":

            if (now_close - open_price) < (-self.stop_loss / 100 * open_price):
                close_long_signal = True
        elif holding_position == "SHORT":
            if (now_close - open_price) > (self.stop_loss / 100 * open_price):
                close_short_signal = True
        return close_long_signal, close_short_signal

    @staticmethod
    def is_weekday(datetime_object) -> bool:
        """

        :param datetime_object:
        :return:
        """
        weekday = datetime_object.weekday() not in (5, 6)
        return weekday

    @staticmethod
    def is_start_trade(datetime_object):
        """

        :param datetime_object:
        :return:
        """
        now_start = (datetime_object.hour == 7) & (datetime_object.minute == 45)
        return now_start

    def run(self):
        """

        :return:
        """
        open_price = 0
        num_of_lot = 0
        holding_position = None
        pos_opened = False
        daily_equity_value_list = [self.initial_capital]
        equity_value_list = []
        dd_pct_list = []
        win_count = 0
        lose_count = 0
        temp_capital = self.initial_capital
        start_index = self.reference_window * self.sub_window_multiplier
        for i in range(start_index, len(self.df)):
            current_datetime = self.df.loc[i, 'date_time']
            now_close = self.df.loc[i, "close"]
            if Backtest.is_start_trade(current_datetime) & Backtest.is_weekday(current_datetime):
                pass
                # open_long_signal, open_short_signal = self.first_trade_of_the_day(i=i)
            elif (current_datetime.hour >= self.start_time) & (current_datetime.hour <= (self.end_time - 1)) & \
                    Backtest.is_weekday(current_datetime):
                open_long_signal, open_short_signal, close_long_signal, close_short_signal = self.signal_generator(i=i)
            else:
                close_long_signal = close_short_signal = True
                open_long_signal = open_short_signal = False
            if holding_position == 'SHORT':
                equity_value = temp_capital + (open_price - now_close) * num_of_lot
            else:
                equity_value = temp_capital + (now_close - open_price) * num_of_lot
            equity_value_list.append(equity_value)
            temp_max_equity = max(equity_value_list)
            dd_pct = (temp_max_equity - equity_value) / temp_max_equity
            dd_pct_list.append(dd_pct)
            if pos_opened:
                close_long_signal, close_short_signal = \
                    self.check_stop_loss(close_long_signal=close_long_signal, close_short_signal=close_short_signal,
                                         holding_position=holding_position, open_price=open_price, now_close=now_close)
                if close_long_signal and (holding_position == 'LONG'):
                    holding_position = None
                    pos_opened = False
                    temp_capital += (now_close - open_price) * num_of_lot - num_of_lot * open_price * self.taker_fee
                    if now_close - open_price > 0:
                        win_count += 1
                    else:
                        lose_count += 1
                    open_price = 0
                    num_of_lot = 0
                elif close_short_signal and (holding_position == 'SHORT'):
                    holding_position = None
                    pos_opened = False
                    temp_capital += (open_price - now_close) * num_of_lot - num_of_lot * open_price * self.taker_fee
                    if open_price - now_close > 0:
                        win_count += 1
                    else:
                        lose_count += 1
                    open_price = 0
                    num_of_lot = 0
            else:
                if open_long_signal:
                    pos_opened = True
                    holding_position = 'LONG'
                    open_price = now_close
                    num_of_lot = equity_value * para_dict["invest_percentage"] / 100 / now_close
                    temp_capital = temp_capital - num_of_lot * open_price * self.taker_fee

                elif open_short_signal:
                    pos_opened = True
                    holding_position = 'SHORT'
                    open_price = now_close
                    num_of_lot = equity_value * para_dict["invest_percentage"] / 100 / now_close
                    temp_capital = temp_capital - num_of_lot * open_price * self.taker_fee

        date_list = self.df.loc[start_index:len(self.df) - 1, 'date_time'].to_list()
        for i in range(1, len(equity_value_list) - 1):
            if date_list[i].date() != date_list[i - 1].date():
                daily_equity_value_list.append(equity_value_list[i - 1])
        equity_value_series = pd.Series(daily_equity_value_list)
        strategy_return_series = equity_value_series.pct_change()
        return_mean_pct = 100 * strategy_return_series.mean()
        return_sd_pct = 100 * strategy_return_series.std()
        sharpe_ratio = return_mean_pct / return_sd_pct * math.sqrt(256)
        neg_strategy_return_series = pd.Series(filter(lambda x: x < 0, strategy_return_series))
        neg_return_sd_pct = 100 * neg_strategy_return_series.std()
        sortino_ratio = return_mean_pct / neg_return_sd_pct * math.sqrt(256)
        net_profit = (equity_value_list[-1] - equity_value_list[0]) / equity_value_list[0] * 100
        mdd_pct = 100 * max(dd_pct_list)
        calmar_ratio = net_profit / mdd_pct
        win_rate = win_count / (win_count + lose_count)
        num_of_trade = win_count + lose_count
        pprint(
            f"{sharpe_ratio = }, {mdd_pct = } , {sortino_ratio = } , {calmar_ratio = } , {net_profit = }, {win_rate = }, {num_of_trade = }")
        res_df = pd.DataFrame({'datetime': date_list, "equity": equity_value_list})
        plt.figure(figsize=(40, 20))
        plt.plot(date_list, [i / self.initial_capital for i in equity_value_list])
        plt.show()
        return res_df


if __name__ == "__main__":
    symbol = "SOLUSDT"
    para_dict = {"strategy_name": "williamsR_MA", "first_window": -85, "second_window": -15,
                 "reference_window": 16, "sub_window_multiplier": 7, "stop_loss": 1, "backtest_year": 1,
                 "invest_percentage": 100, "symbol": symbol, "start_time": 8, "end_time": 17}
    interval = "15m"
    backtest = Backtest(para_dict)
    backtest.get_mongo_kline(interval=interval)
    save_csv = backtest.run()
