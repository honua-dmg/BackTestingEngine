from config import r,r_alg,STOCKS
import pandas as pd
import numpy as np
class CleanData():
    def __init__(self):
        pass

    def transform(self,message):
        pass

    def saveCSV(self,stock,message):
        pass

    def streamToRedis(self,stock,message):
        pass


class Algo1(CleanData):
    def __init__(self):
        super().__init__()
        self.last_traded_vol = 0

    def transform(self,message): # NOTE: keeping track of last_traded_vol here might be dangerous in the event of a crash, keep this in mind.
        """ 
        message format: {'timestamp': '09:40:48', 'stonk': 'NSE:TATAMOTORS', 'last_price': '708.7', 'last_traded_quantity': '1', 'average_traded_price': '708.72', 
                        'volume_traded': '1172839', 'total_buy_quantity': '616320', 'total_sell_quantity': '1060712', 'open': '712.8', 'high': '713.4', 'low': '706.35', 'close': '709.15', 
                        'change': '-0.0634562504406587', 'buy_price_1': '708.5', 'buy_qty_1': '7943', 'buy_orders_1': '21', 'sell_price_1': '708.45', 'sell_qty_1': '50', 'sell_orders_1': '1', 
                        'buy_price_2': '708.4', 'buy_qty_2': '764', 'buy_orders_2': '6', 'sell_price_2': '708.35', 'sell_qty_2': '829', 'sell_orders_2': '10', 'buy_price_3': '708.3', 'buy_qty_3': '2025', 
                        'buy_orders_3': '20', 'sell_price_3': '708.7', 'sell_qty_3': '42', 'sell_orders_3': '1', 'buy_price_4': '708.75', 'buy_qty_4': '1165', 'buy_orders_4': '4', 'sell_price_4': '708.8', 
                        'sell_qty_4': '706', 'sell_orders_4': '8', 'buy_price_5': '708.85', 'buy_qty_5': '1582', 'buy_orders_5': '10', 'sell_price_5': '708.9', 'sell_qty_5': '1606', 'sell_orders_5': '10'}

        output: {ltp,delta,ltp_type}
                none if change in volume is 0
        """

        delta_vol = int(message['volume_traded'])-self.last_traded_vol 
        if delta_vol ==0: return None
        self.last_traded_vol = int(message['volume_traded'])
        #figure out the type:
        buy_prices = np.array([np.float32(message[f'buy_price_{i}']) for i in range(1,6)])
        sell_prices = np.array([np.float32(message[f'sell_price_{i}']) for i in range(1,6)])

        ltp = np.float32(message['last_price'])
        ltp_type = 'b' if np.dot(buy_prices-ltp,(buy_prices-ltp).T) < np.dot(sell_prices-ltp,(sell_prices-ltp).T) else 's'
        spread = max(buy_prices)-min(sell_prices)
        return {'ltp':ltp, 
                'delta':delta_vol, 
                'ltp_type':ltp_type,
                "spread":spread,
                "bid":max(buy_prices),
                "ask":min(sell_prices)}
    

