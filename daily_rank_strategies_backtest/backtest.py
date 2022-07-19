import time
import pandas as pd
import math
from pprint import pprint
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)


class Backtest:
    def __init__(self, para_dict: dict):
        """

        :param para_dict:
        """
        self.df = pd.DataFrame()
        self.initial_capital = 10000.0
        self.total_equity_list = [self.initial_capital]
        self.daily_equity_value_list = [self.initial_capital]
        self.long_only_symbol = "BTCUSDT"
        self.dict_keys_list = ["best", "second_best", "worst", "second_worst"]
        self.equity_dict = {ranking_type: [self.initial_capital / 4] for ranking_type in self.dict_keys_list}
        self.trade_info_dict = {ranking_type: {} for ranking_type in self.dict_keys_list}
        self.trade_symbol_dict = {ranking_type: '' for ranking_type in self.dict_keys_list}
        self.symbol = para_dict["symbol"]
        self.backtest_year = para_dict["backtest_year"]
        self.start_time = para_dict["start_time"]
        self.end_time = para_dict["end_time"]
        self.time_spread = self.end_time - self.start_time
        self.taker_fee = 0.0008
        self.total_equity = self.initial_capital

    def get_mongo_kline(self, interval: str):
        """
        :param interval:
        """
        df_dict = {}
        current_timestamp = time.time()
        start_from = time.time() - 60 * 60 * 24 * 365 * self.backtest_year
        if isinstance(self.symbol, list):
            for sym in self.symbol:
                # TODO : GET DATA FROM YOUR OWN SOURCE, AND SAVE EACH SYMBOL TO df_dict[sym] correspondingly
                df_dict[sym] = pd.DataFrame()
                # Stupid way to convert tiestamp to local timestamp
                df_dict[sym]['timestamp'] += 60 * 60 * 8
                df_dict[sym]["date_time"] = pd.to_datetime(df_dict[sym]['timestamp'], unit='s')
                df_dict[sym] = df_dict[sym][["date_time", "close", "turnover"]]
                df_dict[sym].set_index(keys=["date_time"], inplace=True)
                df_dict[sym].rename(columns={"close": f"{sym}_close", "turnover": f"{sym}_turnover"}, inplace=True)
                df_dict[sym].sort_index(inplace=True)
        bigdata = pd.concat(list(df_dict.values()), axis=1)
        bigdata.dropna(inplace=True)
        bigdata.reset_index(inplace=True)
        self.df = bigdata

    def start_trading(self, i: int):
        """

        :param i:
        :return:
        """
        pct_change_dict = {}
        turn_over_dict = {}
        for sym in self.symbol:
            turn_over_dict[sym] = self.df.iloc[i - 24: i - (24 - self.time_spread) + 2].sum(numeric_only=True)[
                f"{sym}_turnover"]
            pct_change_dict[sym] = (self.df.loc[i - (24 - self.time_spread) + 1, f"{sym}_close"] /
                                    self.df.loc[i - 24, f"{sym}_close"] - 1) * 100 * turn_over_dict[sym]
        weight_turnover = sum(turn_over_dict.values())
        # index is for reference
        # index = weight_turnover / sum(turn_over_dict.values())
        pct_change_dict = {k: pct_change_dict[k] / weight_turnover for k in pct_change_dict}
        sorted_pct_change_dict = {k: v for k, v in sorted(pct_change_dict.items(), key=lambda item: item[1])}
        if self.long_only_symbol in list(sorted_pct_change_dict.keys())[0:2]:
            short_symbol_list = [short_symbol for short_symbol in list(sorted_pct_change_dict.keys())[0:3]
                                 if short_symbol != self.long_only_symbol]
            long_symbol_list = [list(sorted_pct_change_dict.keys())[-1], self.long_only_symbol]
        else:
            short_symbol_list = list(sorted_pct_change_dict.keys())[0:2]
            if "BTCUSDT" == list(sorted_pct_change_dict.keys())[-1]:
                long_symbol_list = list(sorted_pct_change_dict.keys())[-2:]
            else:
                long_symbol_list = [list(sorted_pct_change_dict.keys())[-1], self.long_only_symbol]
        self.trade_symbol_dict["worst"] = short_symbol_list[0]
        self.trade_symbol_dict["second_worst"] = short_symbol_list[1]
        self.trade_symbol_dict["best"] = self.long_only_symbol
        long_symbol_list.remove(self.long_only_symbol)
        self.trade_symbol_dict["second_best"] = long_symbol_list[0]
        # for the worst two symbol
        for ranking_type in self.trade_info_dict:
            close_price = self.df.loc[i, f'{self.trade_symbol_dict[ranking_type]}_close']
            self.trade_info_dict[ranking_type]["open_price"] = close_price
            self.trade_info_dict[ranking_type]["trading_lot"] = self.total_equity / 4 / close_price
            self.trade_info_dict[ranking_type]["temp_capital"] -= self.trade_info_dict[ranking_type]["trading_lot"] * \
                                                                  self.trade_info_dict[ranking_type]["open_price"] * \
                                                                  self.taker_fee

    def end_trade(self, i: int):
        """

        :param i:
        :return:
        """
        for ranking_type in self.trade_info_dict:
            open_price = self.trade_info_dict[ranking_type]["open_price"]
            close_price = self.df.loc[i, f'{self.trade_symbol_dict[ranking_type]}_close']
            trading_lot = self.trade_info_dict[ranking_type]["trading_lot"]
            if "worst" in ranking_type:
                self.trade_info_dict[ranking_type]["temp_capital"] += \
                    (open_price - close_price) * trading_lot - trading_lot * close_price * self.taker_fee

            else:
                self.trade_info_dict[ranking_type]["temp_capital"] += \
                    (close_price - open_price) * trading_lot - trading_lot * close_price * self.taker_fee
        # after closing the position, set all the open_price and trading lot be zero
        self.initialize_dictionary()

    def track_trading(self, i: int):
        """

        :param i:
        :return:
        """
        for ranking_type in self.trade_info_dict:
            trading_lot = self.trade_info_dict[ranking_type]["trading_lot"]
            if trading_lot == 0:
                self.equity_dict[ranking_type].append(self.equity_dict[ranking_type][-1])
            else:
                close_price = self.df.loc[i, f'{self.trade_symbol_dict[ranking_type]}_close']
                open_price = self.trade_info_dict[ranking_type]["open_price"]
                temp_capital = self.trade_info_dict[ranking_type]["temp_capital"]
                if "worst" in ranking_type:
                    self.equity_dict[ranking_type].append(temp_capital + (open_price - close_price) * trading_lot)
                else:
                    self.equity_dict[ranking_type].append(temp_capital + (close_price - open_price) * trading_lot)

    def initialize_dictionary(self):
        """

        :return:
        """
        trade_info_keys_list = ["open_price", "trading_lot", "temp_capital"]
        for ranking_type in self.trade_info_dict:
            for trade_info in trade_info_keys_list:
                self.trade_info_dict[ranking_type][trade_info] = 0 if trade_info != "temp_capital" \
                    else self.total_equity / 4

    def run(self):
        """

        :return:
        """
        dd_pct = 0
        self.initialize_dictionary()
        for i in range(30, len(self.df)):
            current_datetime = self.df.loc[i, 'date_time']
            if current_datetime.hour == self.start_time - 1:
                self.start_trading(i=i)
            elif current_datetime.hour == self.end_time - 1:
                self.end_trade(i=i)
            # elif current_datetime.hour >= self.start_time & current_datetime.hour < self.end_time - 2:
            self.track_trading(i=i)
            self.total_equity_list.append(sum([equity[-1] for equity in list(self.equity_dict.values())]))
            temp_max_equity = max(self.total_equity_list)
            mdd_pct = max(dd_pct, (temp_max_equity - self.total_equity_list[-1]) / temp_max_equity)

            dd_pct = (temp_max_equity - self.total_equity_list[-1]) / temp_max_equity
        date_list = self.df.loc[29:len(self.df) - 1, 'date_time'].to_list()
        for i in range(1, len(self.total_equity_list) - 1):
            if date_list[i].date() != date_list[i - 1].date():
                self.daily_equity_value_list.append(self.total_equity_list[i - 1])

        equity_value_series = pd.Series(self.daily_equity_value_list)
        strategy_return_series = equity_value_series.pct_change()
        return_mean_pct = 100 * strategy_return_series.mean()
        return_sd_pct = 100 * strategy_return_series.std()
        sharpe_ratio = return_mean_pct / return_sd_pct * math.sqrt(365)
        neg_strategy_return_series = pd.Series(filter(lambda x: x < 0, strategy_return_series))
        neg_return_sd_pct = 100 * neg_strategy_return_series.std()
        sortino_ratio = return_mean_pct / neg_return_sd_pct * math.sqrt(365)
        net_profit = (self.total_equity_list[-1] - self.total_equity_list[0]) / self.total_equity_list[0] * 100
        mdd_pct *= 100
        calmar_ratio = net_profit / mdd_pct
        pprint(f"{sharpe_ratio = }, {mdd_pct = } , {sortino_ratio = } , {calmar_ratio = } , {net_profit = }")
        res_df = pd.DataFrame({'datetime': date_list, "equity": self.total_equity_list})
        fig, axes = plt.subplots(4, 1, figsize=(30, 30), sharex=True)
        fig.tight_layout()
        for index, ranking_type in enumerate(list(self.equity_dict.keys())):
            axes[index].plot(date_list, self.equity_dict[ranking_type], label=ranking_type)
        plt.show()
        plt.figure(figsize=(40, 20))
        plt.plot(date_list, [i / self.initial_capital for i in self.total_equity_list])
        plt.show()
        return res_df


if __name__ == "__main__":
    symbol = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT", "DOGEUSDT", "DOTUSDT", "TRXUSDT", "AVAXUSDT",
              "XRPUSDT"]
    para_dict = {"strategy_name": "daily_rank", "start_time": 8, "end_time": 16, "backtest_year": 1, "symbol": symbol}
    interval = "1h"
    backtest = Backtest(para_dict)
    backtest.get_mongo_kline(interval=interval)
    save_csv = backtest.run()

