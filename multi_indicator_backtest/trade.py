from pybit import usdt_perpetual
import os
import pybit.exceptions
import logging
import inspect

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("pybit").setLevel(logging.ERROR)

script_dir = os.path.dirname(os.path.realpath(__file__))
config_filename = f"config.json"
config_path = os.path.join(script_dir, config_filename)

redis_config = read_config_json(topic="redis", config_json_path=config_path)
trading_config = read_config_json(topic="alpha_param", config_json_path=config_path)
setting_config = read_config_json(topic="setting", config_json_path=config_path)

exchange = trading_config['exchange']
asset_type = trading_config['asset_type']
symbol = trading_config['symbol']
period = trading_config['period']
take_profit = trading_config['take_profit']
stop_loss = trading_config['stop_loss']
reference_window = trading_config['reference_window']
sub_window_multiplier = trading_config["sub_window_multiplier"]

strategy_name = setting_config['strategy_name']


class Alpha:
    def __init__(self, api_key: str, api_secret: str, invest_amount: str):
        """

        :param api_key:
        :param api_secret:
        :param invest_amount:
        """
        self.closed_alpha = None
        self.session_auth = usdt_perpetual.HTTP(
            endpoint="https://api.bybit.com",
            api_key=api_key,
            api_secret=api_secret)
        self.api_key = api_key
        self.api_secret = api_secret
        self.invest_amount = invest_amount
        self.position_opened = False
        self.holding_type = None
        self.data_list = self.get_past_kline_data(reference_window=reference_window)
        self.ma_list = self.get_past_kline_data(reference_window=sub_window_multiplier * reference_window)
        self.group_name = f'williamsR'
        self.qty = int((float(invest_amount) / self.get_current_price()) * 1000) / 1000

    def percentage_r_calculator(self):
        """
        Calculate the william R number
        :return:
        """
        percentage_r = -50
        try:
            lowest_price = min([data['low'] for data in self.data_list])
            highest_price = max([data['high'] for data in self.data_list])
            percentage_r = (highest_price - self.data_list[-1]['close']) / (highest_price - lowest_price) * -100
        except BaseException as err:
            print(f"In function {inspect.stack()[0][3]}, Unexpected {err=}, {type(err)=}")
        else:
            return percentage_r

    def ma_calculator(self):
        ma = "UP"
        current_close = self.data_list[-1]['close']
        current_ma = sum([data['high'] for data in self.data_list]) / len([data['high'] for data in self.data_list])
        if current_close > current_ma:
            ma = "UP"
        else:
            ma = "DOWN"
        return ma

    def get_take_profit_price(self, order_side):
        """
        This method generate the take profit and stop loss price base on the orderBook bid/ask and the custom parameters
        """
        orderbook_generator = client_consumer.get_orderbook_history(exchange=exchange, asset_type=asset_type,
                                                                    symbol=symbol)
        message = next(orderbook_generator)
        data = message[0]
        if message:
            best_ask = data['ask'][0][0]
            best_bid = data['bid'][0][0]
        if order_side == "Buy":
            take_profit_price = int(best_ask * (1 + take_profit / 100))
        elif order_side == "Sell":
            take_profit_price = int(best_bid * (1 - take_profit / 100))
        return take_profit_price

    def get_stop_loss_price(self, order_side):
        """
        This method generate the stop loss price base on the orderBook bid/ask and the custom parameters
        """
        orderbook_generator = client_consumer.get_orderbook_history(exchange=exchange, asset_type=asset_type,
                                                                    symbol=symbol)

        message = next(orderbook_generator)
        data = message[0]
        if message:
            best_ask = data['ask'][0][0]
            best_bid = data['bid'][0][0]
        if order_side == "Buy":
            stop_loss_price = int(best_ask * (1 - stop_loss / 100))
        elif order_side == "Sell":
            stop_loss_price = int(best_bid * (1 + stop_loss / 100))
        return stop_loss_price

    def get_past_kline_data(self, reference_window: int):
        """
        This method get all essential data before trading
        """
        past_kline_list = []
        try:
            # TODO : Get historical kline price data
                past_kline_list.append(info)
            past_kline_list = [{key: value for key, value in message.items() if key in ('high', 'close', 'low')}
                               for message in past_kline_list]
        except BaseException as err:
            print(f"In function {inspect.stack()[0][3]}, Unexpected {err=}, {type(err)=}")
        return past_kline_list

    @staticmethod
    def trading_signal_generator(percentage_r: float, ma: str):
        """
        Generate the trading signal,
        by filtering the william R
        :return:
        """
        open_long_signal = (percentage_r < -85) and (ma == "UP")
        close_long_signal = percentage_r > -15
        open_short_signal = (percentage_r > -15) and (ma == "DOWN")
        close_short_signal = percentage_r < -85
        return open_long_signal, close_long_signal, open_short_signal, close_short_signal

    def query_current_position(self):
        """
        This method check current position
        """
        no_position = True
        try:
            position_info = self.session_auth.my_position(symbol=symbol)
            if position_info["ret_msg"] == "OK":
                for position in position_info["result"]:
                    if position["size"] != 0:
                        no_position = False
        except pybit.exceptions.InvalidRequestError as e:
            print(f"Invalid request ERROR when placing open short position, {e}")
        except pybit.exceptions.FailedRequestError as e:
            print(f"Fail request ERROR when placing open short position, {e}")
        except BaseException as err:
            print(f"In function {inspect.stack()[0][3]}, Unexpected {err=}, {type(err)=}")
        return no_position

    def open_long_position(self):
        """
        This method execute the market buy order wtih stop loss and take profit
        """
        order_side = "Buy"
        take_profit_price = self.get_take_profit_price(order_side=order_side) if take_profit != 0 else None
        stop_loss_price = self.get_stop_loss_price(order_side=order_side) if stop_loss != 0 else None
        try:
            response = self.session_auth.place_active_order(symbol=symbol, side=order_side,
                                                            order_type="Market",
                                                            qty=self.qty, time_in_force="GoodTillCancel",
                                                            reduce_only=False,
                                                            close_on_trigger=False,
                                                            order_link_id=generate_uid(
                                                                strategy_name=strategy_name,
                                                                client_api=self.api_key),
                                                            take_profit=take_profit_price,
                                                            stop_loss=stop_loss_price)
        except pybit.exceptions.InvalidRequestError as e:
            print(f"bybit open long position error: {e} , {take_profit_price = } , {stop_loss_price = }")
            return False
        except BaseException as err:
            print(f"In function {inspect.stack()[0][3]}, Unexpected {err=}, {type(err)=}")
            return False
        else:
            if response['ret_msg'] == "OK":
                print(f"Successfully place buy order {response}")
                return True
            else:
                print(f"An ERROR occurred in function {inspect.stack()[0][3]}, bybit ret_msg not OK, unknown error")
                return False

    def open_short_position(self):
        """
        This method execute the market sell order wtih stop loss and take profit
        """
        order_side = "Sell"
        take_profit_price = self.get_take_profit_price(order_side=order_side) if take_profit != 0 else None
        stop_loss_price = self.get_stop_loss_price(order_side=order_side) if stop_loss != 0 else None
        try:
            response = self.session_auth.place_active_order(symbol=symbol, side=order_side,
                                                            order_type="Market",
                                                            qty=self.qty, time_in_force="GoodTillCancel",
                                                            reduce_only=False,
                                                            close_on_trigger=False,
                                                            order_link_id=generate_uid(
                                                                strategy_name=strategy_name,
                                                                client_api=self.api_key),
                                                            take_profit=take_profit_price,
                                                            stop_loss=stop_loss_price)
        except pybit.exceptions.InvalidRequestError as e:
            print(f"Invalid request ERROR when placing open short position, {e}")
            return False
        except pybit.exceptions.FailedRequestError as e:
            print(f"Fail request ERROR when placing open short position, {e}")
            return False
        except BaseException as err:
            print(f"In function {inspect.stack()[0][3]}, Unexpected {err=}, {type(err)=}")
        else:
            if response['ret_msg'] == "OK":
                print(f"Successfully place sell order {response}")
                return True
            else:
                print(f"An ERROR occurred in function {inspect.stack()[0][3]}, bybit ret_msg not OK, unknow error")
                return False

    def close_long_position(self):
        """
        This method close long position
        """
        try:
            response = self.session_auth.place_active_order(symbol=symbol, side="Sell",
                                                            order_type="Market",
                                                            qty=self.qty, time_in_force="GoodTillCancel",
                                                            reduce_only=True,
                                                            close_on_trigger=False,
                                                            order_link_id=generate_uid(
                                                                strategy_name=strategy_name,
                                                                client_api=self.api_key))
        except pybit.exceptions.InvalidRequestError as e:
            print(f"Invalid request ERROR when placing close long position, {e}")
            return False
        except pybit.exceptions.FailedRequestError as e:
            print(f"Fail request ERROR when placing close long position, {e}")
            return False
        except BaseException as err:
            print(f"In function {inspect.stack()[0][3]}, Unexpected {err=}, {type(err)=}")
        else:
            if response['ret_msg'] == "OK":
                print(f"Successfully place close long position {response}")
                return True
            else:
                print(f"bybit ret_msg not OK, unknow ERROR occurred")
                return False

    def close_short_position(self):
        """
        This method close short position
        """
        try:
            response = self.session_auth.place_active_order(symbol=symbol, side="Buy",
                                                            order_type="Market",
                                                            qty=self.qty, time_in_force="GoodTillCancel",
                                                            reduce_only=True,
                                                            close_on_trigger=False)
        except pybit.exceptions.InvalidRequestError as e:
            print(f"Invalid request ERROR when placing close short position, {e}")
            return False
        except pybit.exceptions.FailedRequestError as e:
            print(f"Fail request ERROR when placing close short position, {e}")
            return False
        except BaseException as err:
            print(f"In function {inspect.stack()[0][3]}, Unexpected {err=}, {type(err)=}")
        else:
            if response['ret_msg'] == "OK":
                print(f"Successfully place close short position {response}")
                return True
            else:
                print(f"bybit ret_msg not OK, unknow ERROR occurred")
                return False

    def run(self):
        """
        Start the trading program
        :return:
        """
        # Get data through the redis
        # TODO : For loop your data source
            try:
                # Extract essential key value to
                ohl_dict = {key: value for key, value in message.items() if key in ('high', 'close', 'low')}
                # Append the new kline data into self.data_list for calculate the William_R
                self.data_list.append(ohl_dict)
                self.ma_list.append(ohl_dict)
                if len(self.data_list) > reference_window:
                    self.data_list = self.data_list[1:]
                if len(self.ma_list) > reference_window * sub_window_multiplier:
                    self.ma_list = self.ma_list[1:]
                    percentage_r = self.percentage_r_calculator()
                    ma = self.ma_calculator()
                    open_long_signal, close_long_signal, open_short_signal, close_short_signal \
                        = Alpha.trading_signal_generator(percentage_r, ma)
                    # If self.position_opened == True, check if it has close long or close signal,
                    # if yes, follow the signal to close current position
                    if self.position_opened:
                        if (close_short_signal is True) and (self.holding_type == 'SHORT'):
                            # Close shorting position
                            if self.close_short_position():
                                self.position_opened = False
                                self.holding_type = None
                        elif (close_long_signal is True) and (self.holding_type == 'LONG'):
                            # Close longing position
                            if self.close_long_position():
                                self.position_opened = False
                                self.holding_type = None
                    # Checkout position size, if self.query_current_position() return True,
                    # it means no position currently has
                    if self.query_current_position():
                        self.position_opened = False
                    # If self.position_opened == False, check if it has open long or short signal,
                    # if yes, follow the signal to execute trade
                    if not self.position_opened:
                        if open_long_signal:
                            # Open long position
                            if self.open_long_position():
                                self.holding_type = 'LONG'
                                self.position_opened = True
                        elif open_short_signal:
                            # Open short position
                            if self.open_short_position():
                                self.holding_type = 'SHORT'
                                self.position_opened = True
            except Exception as err:
                print(f"In function {inspect.stack()[0][3]}, Super Unexpected {err=}, {type(err)=}")