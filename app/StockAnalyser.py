import pandas as pd
import numpy as np
from cleanData import Algo1
import pyqtgraph as pg
import datetime as dt
class Cumulative_Support():
    def __init__(self,vol=True):
        self.aggDf = pd.DataFrame(columns=['buy-vol', 'sell-vol'])
        self.aggDf.index.name = 'ltp' # Name the index for clarity
        self.ltpDf = pd.DataFrame(columns=['time', 'ltp', 'buy-vol', 'sell-vol', 'type'])

   

 
        self.lowHighdf = [pd.DataFrame(),pd.DataFrame()]
        self.highLowdf = [pd.DataFrame(),pd.DataFrame()]
        self.combineddf = [pd.DataFrame(dtype='Int64'),pd.DataFrame(dtype='Int64')]
        self.total = pd.DataFrame(dtype='Int64')
        self.lowHighMaxes = [pd.DataFrame(columns=['second','first']),pd.DataFrame(columns=['second','first'])]
        self.HighlowMaxes = [pd.DataFrame(columns=['second','first']),pd.DataFrame(columns=['second','first'])]
        self.volOrQty = vol

        self.voldiff_buy = pd.DataFrame()
        self.voldiff_sell = pd.DataFrame()

        self.cleaner = Algo1()

        self.normalised = []
    def update_volDiff(self,size,vol_df,vol_type='buy-vol'):
        
        if self.ltpDf.size<size:
            update = np.nan
        else:
            #update = self.ltpDf['buy-vol'][-size:].sum() - self.ltpDf['sell-vol'][-size:].sum()
            update = self.ltpDf[vol_type].ewm(span=70).mean().iloc[-1]
        

        vol_df.loc[self.ltpDf.index[-1],0] = update
        

    def update_df(self,last_traded_time,ltp:int,delta:int,type:str):
        """ 
        updates the aggregrated dataframe and the ltp dataframe. 

        args:
            last_traded_time
            ltp
            delta   : change in total volume traded
            type    : buy or sell
        
        returns:
            None
        """
        ltp = int(ltp)
        if self.aggDf.empty:
            min_ltp = ltp
            max_ltp = ltp
        else:
            current_min_ltp = self.aggDf.index.min()
            current_max_ltp = self.aggDf.index.max()
            min_ltp = min(current_min_ltp, ltp)
            max_ltp = max(current_max_ltp, ltp)

        # Create the complete new index range
        desired_index = pd.Index(range(min_ltp, max_ltp + 1), name='ltp')
       

        # Reindex the DataFrame to the desired range
        # fill_value=0 will initialize newly introduced rows with 0
        self.aggDf = self.aggDf.reindex(desired_index, fill_value=0)
        if type == 's':
            self.aggDf.loc[ltp, 'sell-vol'] += delta
        else: # type == 'b'
            self.aggDf.loc[ltp, 'buy-vol'] += delta


        # update ltp data stream. 
        ltp = float(ltp)
        new_record = {
                        'time'      :[last_traded_time],
                        'ltp'       :[ltp],
                        'buy-vol'   :[0],
                        'sell-vol'  :[delta],
                        #'diff'      : diff
            } if type=='s' else  {
                        'time'      :[last_traded_time],
                        'ltp'       :[ltp],
                        'buy-vol'   :[delta],
                        'sell-vol'  :[0],
                        #'diff'      : diff
            }
        #print(f'data added: {new_record} type ltp: {self.ltpDf["ltp"].dtype}')

        self.ltpDf = pd.concat([self.ltpDf, pd.DataFrame(new_record)], ignore_index=True)
        #print(f'ltpDf shape: {self.ltpDf.shape} ltpDf columns: {self.ltpDf.columns} ltpDf index: {self.ltpDf.index}')
        self.update_volDiff(50,self.voldiff_buy,'buy-vol')
        self.update_volDiff(20,self.voldiff_sell,'sell-vol')
        #self.update_volDiff(300,self.voldiff_300,'buy-vol')
      
    
    def signal(self,):
        """
        normalises and finds the cumulative means of the buy volumes.
        """
        types=['buy','sell']
        # if the 
        for index in range(2):
            if len(self.aggDf[self.aggDf[f'{types[index]}-vol']>0]) ==0:
                pd.concat([self.lowHighMaxes[index],pd.DataFrame([[np.nan] * len(self.lowHighMaxes[index].columns)],columns=self.lowHighMaxes[index].columns)])
                pd.concat([self.HighlowMaxes[index],pd.DataFrame([[np.nan] * len(self.lowHighMaxes[index].columns)],columns=self.HighlowMaxes[index].columns)])
                self.lowHighdf[index] = pd.concat(axis=1,objs=[self.lowHighdf[index],pd.DataFrame([[np.nan]], index=[self.ltpDf.index[-1]])]).reindex(self.aggDf.index)
                self.highLowdf[index] = pd.concat(axis=1,objs=[self.highLowdf[index],pd.DataFrame([[np.nan]], index=[self.ltpDf.index[-1]])]).reindex(self.aggDf.index)
                self.combineddf[index] = pd.concat(axis=1,objs=[self.combineddf[index],pd.DataFrame([[np.nan]], index=[self.ltpDf.index[-1]])]).reindex(self.aggDf.index)
                return

            if self.volOrQty:
                self.aggby = 1
            else:
                self.aggby = self.aggDf.index

            avg = (self.aggDf[f'{types[index]}-vol'].mul(self.aggby)).sum()/len(self.aggDf[self.aggDf[f'{types[index]}-vol']>0]) # count only those who contributed.
            lowerbound = self.aggDf[self.aggDf[f'{types[index]}-vol'] != 0].index[0] #why are we doing this??
            upperbound = self.aggDf[self.aggDf[f'{types[index]}-vol'] != 0].index[-1]
            # find the fractional deviation from the average for each ltp and cumsum that shit
            lowHigh = pd.DataFrame(((self.aggDf[f'{types[index]}-vol'].mul(self.aggby))/avg - 1).loc[lowerbound:upperbound].expanding().sum(),index =range(lowerbound,upperbound+1)).reindex(self.aggDf.index).astype(float)
            highLow = pd.DataFrame(((self.aggDf[f'{types[index]}-vol'].mul(self.aggby))/avg - 1).loc[lowerbound:upperbound].iloc[::-1].expanding().sum().iloc[::-1],index =range(lowerbound,upperbound+1)).reindex(self.aggDf.index).astype(float)
            #print(f"{lowHigh[lowHigh.columns[0]].nlargest(2).index.to_list()} {highLow[highLow.columns[0]].nlargest(2).index.to_list()}")
            # we need to append the top 2 of each. 
            self.lowHighMaxes[index].loc[self.ltpDf.index[-1],['second','first']] = lowHigh[lowHigh.columns[0]].nlargest(2).index.to_list() # idk if the to_list part is necessary
            self.HighlowMaxes[index].loc[self.ltpDf.index[-1],['second','first']] = highLow[highLow.columns[0]].nlargest(2).index.to_list()
            combined = lowHigh.map(lambda x: 0 if x<0 else 1)+2*highLow.map(lambda x: 0 if x<0 else 1)
            #combineBuySell.append(combined)
            self.combineddf[index] =pd.concat(
                        axis=1,
                        objs=[self.combineddf[index],combined]
                        ).reindex(self.aggDf.index)

            # we need to append lowHigh and Highlow to self.LowHighdf and self.HighLowdf
            #self.lowHighdf[index] = pd.concat(axis=1,objs=[self.lowHighdf[index],lowHigh.map(lambda x: 0 if x<0 else 1)]).reindex(self.aggDf.index)
            #self.highLowdf[index] = pd.concat(axis=1,objs=[self.highLowdf[index],highLow.map(lambda x: 0 if x<0 else 1)]).reindex(self.aggDf.index)


        if len(self.aggDf) <2:
            self.total= pd.concat(axis=1,
                                    objs=[self.total,
                                            pd.DataFrame([[np.nan]], index=[self.ltpDf.index[-1]],columns=['vol'])]
                                    ).reindex(self.aggDf.index)




    def parse(self,message):
        try:
            _,ltp,delta,ltp_type = self.cleaner.transform(message).values()
        except (TypeError,AttributeError):
            return
        #print(type(ltp),type(delta),type(ltp_type))
        self.update_df(message['timestamp'],ltp,delta,ltp_type)  
        self.signal()


# We'll keep initial allocations relatively small to save memory, 
# because our new logic will dynamically grow them when needed.


INITIAL_HEIGHT = 15000 
INITIAL_WIDTH = 400

class Delta_analysis():
    def __init__(self, volOrQty=True):
        self.cleaner = Algo1() 
        self.volOrQty = volOrQty
        self.HEIGHT = INITIAL_HEIGHT
        self.WIDTH = INITIAL_WIDTH
        
        # Initialize with NaN to guarantee transparent backgrounds
        self.highLow = {'buy': np.full((self.HEIGHT, self.WIDTH), np.nan), 'sell': np.full((self.HEIGHT, self.WIDTH), np.nan)}
        self.lowHigh = {'buy': np.full((self.HEIGHT, self.WIDTH), np.nan), 'sell': np.full((self.HEIGHT, self.WIDTH), np.nan)}
        
        self.aggdf_buy = np.zeros(self.WIDTH)
        self.aggdf_sell = np.zeros(self.WIDTH)
        self.ltpdf = np.zeros((self.HEIGHT, 4)) 

        self.curr_time_idx = 0  
        self.base_ltp = None    
        self.lowHighMaxes = {'buy': [], 'sell': []}
        self.highLowMaxes = {'buy': [], 'sell': []}

    def _ensure_time_bounds(self):
        if self.curr_time_idx >= self.HEIGHT:
            pad_size = 5000 
            self.ltpdf = np.pad(self.ltpdf, ((0, pad_size), (0, 0)), 'constant')
            
            # Pad with NaN
            for k in ['buy', 'sell']:
                self.highLow[k] = np.pad(self.highLow[k], ((0, pad_size), (0, 0)), 'constant', constant_values=np.nan)
                self.lowHigh[k] = np.pad(self.lowHigh[k], ((0, pad_size), (0, 0)), 'constant', constant_values=np.nan)
            self.HEIGHT += pad_size

    def _ensure_price_bounds(self, ltp):
        ltp = int(ltp)
        if self.base_ltp is None:
            self.base_ltp = ltp - (self.WIDTH // 2)
            return ltp - self.base_ltp
            
        target_idx = ltp - self.base_ltp

        if target_idx < 0:
            pad_size = abs(target_idx) + 50 
            self.aggdf_buy = np.pad(self.aggdf_buy, (pad_size, 0), 'constant')
            self.aggdf_sell = np.pad(self.aggdf_sell, (pad_size, 0), 'constant')
            for k in ['buy', 'sell']:
                self.highLow[k] = np.pad(self.highLow[k], ((0, 0), (pad_size, 0)), 'constant', constant_values=np.nan)
                self.lowHigh[k] = np.pad(self.lowHigh[k], ((0, 0), (pad_size, 0)), 'constant', constant_values=np.nan)
            self.base_ltp -= pad_size
            self.WIDTH += pad_size
            target_idx = ltp - self.base_ltp

        elif target_idx >= self.WIDTH:
            pad_size = (target_idx - self.WIDTH) + 50
            self.aggdf_buy = np.pad(self.aggdf_buy, (0, pad_size), 'constant')
            self.aggdf_sell = np.pad(self.aggdf_sell, (0, pad_size), 'constant')
            for k in ['buy', 'sell']:
                self.highLow[k] = np.pad(self.highLow[k], ((0, 0), (0, pad_size)), 'constant', constant_values=np.nan)
                self.lowHigh[k] = np.pad(self.lowHigh[k], ((0, 0), (0, pad_size)), 'constant', constant_values=np.nan)
            self.WIDTH += pad_size

        return target_idx

    def update_CNDM(self, agg_arr, direction):
        mask = agg_arr > 0
        if not np.any(mask):
            return np.full_like(agg_arr, np.nan, dtype=float)

        prices = np.arange(self.base_ltp, self.base_ltp + self.WIDTH)
        aggby = np.ones_like(prices) if self.volOrQty else prices
        
        average = np.sum(agg_arr[mask] * aggby[mask]) / np.sum(mask)

        valid_indices = np.where(mask)[0]
        lower = valid_indices[0]
        upper = valid_indices[-1]

        trimmed_agg = agg_arr[lower:upper+1]
        trimmed_aggby = aggby[lower:upper+1]
        trimmed_signal = (trimmed_agg * trimmed_aggby) / average - 1.0

        if direction == 'lowHigh':
            result = trimmed_signal.cumsum()
        else:
            result = trimmed_signal[::-1].cumsum()[::-1]

        full = np.full_like(agg_arr, np.nan, dtype=float)
        full[lower:upper+1] = result
        return full

    def update_signals(self):
        for k, agg_arr in [('buy', self.aggdf_buy), ('sell', self.aggdf_sell)]:
            lh = self.update_CNDM(agg_arr, 'lowHigh')
            hl = self.update_CNDM(agg_arr, 'highLow')

            self.lowHigh[k][self.curr_time_idx] = lh
            self.highLow[k][self.curr_time_idx] = hl

            if np.any(~np.isnan(lh)):
                lh_safe = np.nan_to_num(lh, nan=-np.inf)
                top2_lh_idx = np.argsort(lh_safe)[-2:][::-1] 
                self.lowHighMaxes[k].append([self.base_ltp + i for i in top2_lh_idx])
            else:
                self.lowHighMaxes[k].append([np.nan, np.nan])

            if np.any(~np.isnan(hl)):
                hl_safe = np.nan_to_num(hl, nan=-np.inf)
                top2_hl_idx = np.argsort(hl_safe)[-2:][::-1]
                self.highLowMaxes[k].append([self.base_ltp + i for i in top2_hl_idx])
            else:
                self.highLowMaxes[k].append([np.nan, np.nan])

    def parse(self, message):
        try:
            _, ltp, delta, ltp_type = self.cleaner.transform(message).values()
        except (TypeError, AttributeError, ValueError):
            return

        ltp = float(ltp)
        delta = float(delta)

        self._ensure_time_bounds()
        target_idx = self._ensure_price_bounds(ltp)

        if ltp_type == 'b':
            self.ltpdf[self.curr_time_idx] = [ltp, delta, 0, 0]
            self.aggdf_buy[target_idx] += delta
        elif ltp_type == 's':
            self.ltpdf[self.curr_time_idx] = [ltp, 0, delta, 1]
            self.aggdf_sell[target_idx] += delta

        self.update_signals()
        self.curr_time_idx += 1