import pandas as pd
import numpy as np
from cleanData import Algo1
import pyqtgraph as pg
class Cumulative_Support():
    def __init__(self,detection_type='buy',vol=True):
        self.aggDf = pd.DataFrame(columns=['buy-vol', 'sell-vol'])
        self.aggDf.index.name = 'ltp' # Name the index for clarity
        self.ltpDf = pd.DataFrame(columns=['time', 'ltp', 'buy-vol', 'sell-vol', 'type'])

        self.vol = f'{detection_type}-vol'

 
        self.lowHighdf = [pd.DataFrame(),pd.DataFrame()]
        self.highLowdf = [pd.DataFrame(),pd.DataFrame()]
        self.lowHighMaxes = [pd.DataFrame(columns=['second','first']),pd.DataFrame(columns=['second','first'])]
        self.HighlowMaxes = [pd.DataFrame(columns=['second','first']),pd.DataFrame(columns=['second','first'])]
        self.volOrQty = vol

        self.voldiff_50 = pd.DataFrame()
        self.voldiff_20 = pd.DataFrame()
        self.voldiff_300 = pd.DataFrame()

        self.cleaner = Algo1()

    def update_volDiff(self,size,vol_df,vol_type='buy-vol'):
        
        if self.ltpDf.size<size:
            update = np.nan
        else:
            update = self.ltpDf['buy-vol'][-size:].sum() - self.ltpDf['sell-vol'][-size:].sum()
            #update = self.ltpDf[vol_type][-size:].sum()
        

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
        self.update_volDiff(50,self.voldiff_50,'buy-vol')
        self.update_volDiff(20,self.voldiff_20,'sell-vol')
        #self.update_volDiff(300,self.voldiff_300)
    
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
                return

            if self.volOrQty:
                self.aggby = 1
            else:
                self.aggby = self.aggDf.index

            avg = (self.aggDf[f'{types[index]}-vol'].mul(self.aggby)).sum()/len(self.aggDf[self.aggDf[f'{types[index]}-vol']>0]) # count only those who contributed.
            lowerbound = self.aggDf[self.aggDf[f'{types[index]}-vol'] != 0].index[0]
            upperbound = self.aggDf[self.aggDf[f'{types[index]}-vol'] != 0].index[-1]
            # find the fractional deviation from the average for each ltp and cumsum that shit
            lowHigh = pd.DataFrame(((self.aggDf[f'{types[index]}-vol'].mul(self.aggby))/avg - 1).loc[lowerbound:upperbound].expanding().sum(),index =range(lowerbound,upperbound+1)).reindex(self.aggDf.index).astype(float)
            highLow = pd.DataFrame(((self.aggDf[f'{types[index]}-vol'].mul(self.aggby))/avg - 1).loc[lowerbound:upperbound].iloc[::-1].expanding().sum().iloc[::-1],index =range(lowerbound,upperbound+1)).reindex(self.aggDf.index).astype(float)
            #print(f"{lowHigh[lowHigh.columns[0]].nlargest(2).index.to_list()} {highLow[highLow.columns[0]].nlargest(2).index.to_list()}")
            # we need to append the top 2 of each. 
            self.lowHighMaxes[index].loc[self.ltpDf.index[-1],['second','first']] = lowHigh[lowHigh.columns[0]].nlargest(2).index.to_list() # idk if the to_list part is necessary
            self.HighlowMaxes[index].loc[self.ltpDf.index[-1],['second','first']] = highLow[highLow.columns[0]].nlargest(2).index.to_list()

            with open('test.txt','a') as f:
                print(f"{highLow.to_dict()}",file=f)
            # we need to append lowHigh and Highlow to self.LowHighdf and self.HighLowdf
            self.lowHighdf[index] = pd.concat(axis=1,objs=[self.lowHighdf[index],lowHigh.map(lambda x: 0 if x<0 else 1)]).reindex(self.aggDf.index)
            self.highLowdf[index] = pd.concat(axis=1,objs=[self.highLowdf[index],highLow.map(lambda x: 0 if x<0 else 1)]).reindex(self.aggDf.index)
            #print(f"lowHighdf shape: {self.lowHighdf.shape} highLowdf shape: {self.highLowdf.shape}")



    def parse(self,message):
        try:
            ltp,delta,ltp_type = self.cleaner.transform(message).values()
        except (TypeError,AttributeError):
            return
        #print(type(ltp),type(delta),type(ltp_type))
        self.update_df(message['timestamp'],ltp,delta,ltp_type)  
        self.signal()

