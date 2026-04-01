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
            ltp,delta,ltp_type = self.cleaner.transform(message).values()
        except (TypeError,AttributeError):
            return
        #print(type(ltp),type(delta),type(ltp_type))
        self.update_df(message['timestamp'],ltp,delta,ltp_type)  
        self.signal()

HEIGHT = 15000
WIDTH = 400

class Delta_analysis():
    """
    A new an improved version of the CNDM model. This class promises to be more memory and time efficient.

    Overview of process:
    1. parse incoming message using cleaner module
    2. update ltpdf and aggdf 
    3. update highlow and lowghigh with the new data. 
    
    """
    def __init__(self):

        self.cleaner = Algo1()
        self.highLow = { 'buy':np.zeros((HEIGHT,WIDTH)),
                    'sell':np.zeros((HEIGHT,WIDTH))}
        
        self.lowHigh = { 'buy':np.zeros((HEIGHT,WIDTH)),
                    'sell':np.zeros((HEIGHT,WIDTH))}
        
        
        self.aggdf_sell = np.zeros((WIDTH,1)) 
        self.aggdf_buy = np.zeros((WIDTH,1)) 
        self.ltpdf = np.zeros((HEIGHT,4)) #,ltp, buy-vol, sell-vol, type
        self.ltpdf_cols = {
            'time':0,
            'ltp':1,
            'buy-vol':2,
            'sell-vol':3,
            'type':4
        }

        self.curr_ltp_index = 0
        self.indexes = { #dealing with aggdf, and related components
            'curr_ltp_index': 200,  'curr_ltp':0,
            'max_ltp_index': 400,   'max_ltp':0,
            'min_ltp_index': 0,     'min_ltp':0
        }



    def _rebalance_df(self, arr: np.ndarray, curr_index: int, k: int = 50):
        """
        Rebalance an array by adding `k` rows on the side opposite the nearest boundary.

        If curr_index is closer to the top (last row), append k empty rows at the end.
        If curr_index is closer to the bottom (row 0), prepend k empty rows at the start.

        Returns:
            new_arr: rebalanced array with shape (old_rows + k, *arr.shape[1:])
            new_index: updated index pointer in new_arr
        """
        if arr.ndim == 0:
            raise ValueError("arr must have at least 1 dimension")
        if k < 0:
            raise ValueError("k must be >= 0")
        if not (0 <= curr_index < arr.shape[0]):
            raise IndexError("curr_index out of bounds")

        old_rows = arr.shape[0]
        new_shape = (old_rows + k, *arr.shape[1:])
        new_arr = np.zeros(new_shape, dtype=arr.dtype)

        dist_to_bottom = curr_index
        dist_to_top = (old_rows - 1) - curr_index

        # Closer to top boundary -> keep data at start, leave empty rows at end.
        if dist_to_top <= dist_to_bottom:
            new_arr[:old_rows, ...] = arr
            new_index = curr_index
        # Closer to bottom boundary -> shift data down, leave empty rows at start.
        else:
            new_arr[k:k + old_rows, ...] = arr
            new_index = curr_index + k

        return new_arr, new_index
    
    def rebalancedfs(self,df,type:str):
        if type =='row':
            return self._rebalance_df(df,self.indexes['curr_ltp_index'],k=50)
        else:
            df,new_index= self._rebalance_df(df.T,df.shape[1]-1,k=50)
            return df.T,new_index
        
    def rebalance(self,):
        """ Rebalance all dataframes and update indexes accordingly. """
        print()
        #self.ltpdf,self.curr_ltp_index = self.rebalancedfs(self.ltpdf,'row')
        self.aggdf_buy,self.indexes['curr_ltp_index'] = self.rebalancedfs(self.aggdf_buy,'row')
        self.aggdf_sell,self.indexes['curr_ltp_index'] = self.rebalancedfs(self.aggdf_sell,'row')
        self.highLow['buy'],_ = self.rebalancedfs(self.highLow['buy'],'col')
        self.highLow['sell'],_ = self.rebalancedfs(self.highLow['sell'],'col')
        self.lowHigh['buy'],_ = self.rebalancedfs(self.lowHigh['buy'],'col')
        self.lowHigh['sell'],_ = self.rebalancedfs(self.lowHigh['sell'],'col')

    def update_ltp(self,time, ltp,delta,delta_type):
        """Update ltp df
        delt_type = 'b' or 's' for buy or sell respectively.
        stored as 0, 1 in the ltp df for memory efficiency.
        """
        # we're not storing time right now cuz of how we're keeping track of data (numpy arrays)

        
        if delta_type == 'b':
            self.ltpdf[self.curr_ltp_index]= [ltp,delta,0,0] # 0- BUY 1- SELL
        elif delta_type == 's':
            self.ltpdf[self.curr_ltp_index]= [ltp,0,delta,1]

        #print('updated ltp_df')
        self.curr_ltp_index+=1
        if self.curr_ltp_index >= HEIGHT:
            self.ltpdf,self.curr_ltp_index = self.rebalancedfs(self.ltpdf,'col')
            self.highLow['buy'],_ = self.rebalancedfs(self.highLow['buy'],'col')
            self.highLow['sell'],_ = self.rebalancedfs(self.highLow['sell'],'col')
            self.lowHigh['buy'],_ = self.rebalancedfs(self.lowHigh['buy'],'col')
            self.lowHigh['sell'],_ = self.rebalancedfs(self.lowHigh['sell'],'col')
            print('rebalanced dfs due to ltp_df overflow.')




    def update_agg(self,ltp,delta,delta_type):
        """ Update agg df, and indexes"""
        ltp = int(ltp) #we're aggregrating ltp by integer values.
        if self.indexes['curr_ltp'] == 0: # this is the first ever update, we need to initialize indexes
            self.indexes['curr_ltp'] = ltp
            self.indexes['max_ltp'] = ltp
            self.indexes['min_ltp'] = ltp
        
        index = ltp-self.indexes['curr_ltp'] + self.indexes['curr_ltp_index']

        if delta_type == 'b':
            self.aggdf_buy[index] += delta
        else:
            self.aggdf_sell[index] += delta

        #print(f'updated agg_df at index {index} for ltp {ltp} with delta {delta} and type {delta_type}')



        # update indexes curr_ltp index,value
        self.indexes['curr_ltp'] = ltp
        self.indexes['curr_ltp_index'] = index
        
        # rebalance if necessary
        cols = self.aggdf_buy.shape[0]
        if self.indexes['curr_ltp_index'] < 10 or self.indexes['curr_ltp_index'] > cols-10:
            print('triggering rebalance from update_agg with index:', self.indexes['curr_ltp_index'])
            print(f'current ltp: {self.indexes["curr_ltp"]} current index: {self.indexes["curr_ltp_index"]} upperbound: {cols} lowerbound: 0')
            self.rebalance()
            #print('rebalancing.')

        # update max and min ltp and their indexes
        if ltp> self.indexes['max_ltp']:
            self.indexes['max_ltp'] = ltp
            self.indexes['max_ltp_index'] = index
        elif ltp< self.indexes['min_ltp']:
            self.indexes['min_ltp'] = ltp
            self.indexes['min_ltp_index'] = index


        


    def update_CNDM(self,df,direction):
        """ Update in one direction"""
        mask = df > 0

        # bounds
        rows = np.any(mask, axis=1)
        lower = np.argmax(rows)
        upper = len(df) - 1 - np.argmax(rows[::-1])

        # correct average
        average = np.sum(df, axis=0) / np.sum(mask, axis=0)

        # compute only where valid
        signal = np.zeros_like(df, dtype=float)
        signal[mask] = df[mask] / average - 1

        # trim
        trimmed = signal[lower:upper+1]

        # directional cumsum
        if direction == 'lowHigh':
            result = trimmed.cumsum(axis=0)
        else:
            result = trimmed[::-1].cumsum(axis=0)[::-1]

        # reinsert
        full = np.zeros_like(df, dtype=float)
        full[lower:upper+1] = result

        return full
   
    
    def update_highLow_lowHigh(self,):
        """ Update highLow and lowHigh with the new data in aggdf. We only need to update the range between min and max ltp."""
        
        # we subtract index by 1 cuz we're updating it early on in update_ltp
        self.lowHigh['buy'][self.curr_ltp_index-1] = self.update_CNDM(self.aggdf_buy,'lowHigh').reshape(-1)
        self.lowHigh['sell'][self.curr_ltp_index-1] = self.update_CNDM(self.aggdf_sell,'lowHigh').reshape(-1)
        self.highLow['buy'][self.curr_ltp_index-1] =self.update_CNDM(self.aggdf_buy,'highLow').reshape(-1)
        self.highLow['sell'][self.curr_ltp_index-1] = self.update_CNDM(self.aggdf_sell,'highLow').reshape(-1)

    def update_dfs(self,time, ltp,delta,delta_type):
        """ given a df, it handles appending the new data and handles rebalancing if necessary. """

        # === ltp df update ===
        self.update_ltp(time, ltp,delta,delta_type)

         # === agg df update ===
        self.update_agg(ltp,delta,delta_type)

        # === cumulative df update ===
        self.update_highLow_lowHigh()

        # === highlow and lowhigh update ===
        pass


    def parse(self,message):
        try:
            time,ltp,delta,ltp_type = self.cleaner.transform(message).values()
        except (TypeError,AttributeError) as e:
            # volume change is zero
            return
            #print('we got an error:', e)
            #return
        self.update_dfs(time,ltp,delta,ltp_type)
        

