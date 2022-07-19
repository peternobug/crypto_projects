import ta
import pandas as pd


class Alpha:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def add_indicator(self, strategy_name: str, reference_window: int, sub_window_multiplier: int):
        getattr(self, strategy_name)(strategy_name, reference_window, sub_window_multiplier)
        return self.df

    def williamsR(self, strategy_name: str, reference_window: int, sub_window_multiplier: int):
        self.df[strategy_name.lower()] = ta.momentum.WilliamsRIndicator \
            (high=self.df["high"], low=self.df["low"], close=self.df["close"], lbp=reference_window).williams_r()

    def williamsR_MA(self, strategy_name: str, reference_window: int, sub_window_multiplier: int):
        self.df[strategy_name.lower().split('_')[0]] = ta.momentum.WilliamsRIndicator \
            (high=self.df["high"], low=self.df["low"], close=self.df["close"], lbp=reference_window).williams_r()
        self.df[strategy_name.lower().split('_')[1]] = \
            ta.trend.SMAIndicator(self.df["close"], window=reference_window*sub_window_multiplier).sma_indicator()

    def RSI(self, strategy_name: str, reference_window: int, sub_window_multiplier: int):
        self.df[strategy_name.lower()] = ta.momentum.RSIIndicator(close=self.df["close"], window=reference_window).rsi()
