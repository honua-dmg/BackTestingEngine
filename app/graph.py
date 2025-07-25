import numpy as np
from StockAnalyser import Cumulative_Support
import pyqtgraph as pg
import pandas as pd
import time
def graph(instance):
    
    def addHeatMap(plot,data):
        # Create a colormap with transparency
        """
        colors = [
            (1, (0, 255, 0, 200)), # Green with light alpha
            (0, (255, 0, 0, 200))    # Red with light alpha
        ]
            
        cmap = pg.ColorMap(pos=np.array([c[0] for c in colors]), color=np.array([c[1] for c in colors], dtype=np.ubyte))
        img = pg.ImageItem(data)
        img.setOpacity(.5)
        lut = cmap.getLookupTable(nPts=256, alpha=True)  # Enable alpha
        img.setLookupTable(lut)
        plot.addItem(img)
        return img"""
        
        lut = np.array([
            [254,   79,   92,   200],  # 0: red (0,0)
            [198, 158,   81,   200],  # 1: brown   (0,1)
            [146, 194,   79,   200],  # 2: green (1,0)
            [7,   235, 59,   200],  # 3: neon green  (1,1)
        ], dtype=np.ubyte)

        # Create the ImageItem
        img = pg.ImageItem(data)

        # Assign the LUT
        img.setLookupTable(lut)

        # No need for setOpacity()—alpha comes from LUT
        plot.addItem(img)
        return img
    
    def addlinePlot(plot,linewidth,data:pd.Series,name,color=None):
        x,y = getXY(data)
        if color==None:
            return plot.plot(x,y,pen=pg.mkPen( width=linewidth,name=name))
        return plot.plot(x,y,pen=pg.mkPen(color=color, width=linewidth,name=name))
    
    def getXY(data: pd.Series):
        if data is None or data.empty or data.shape == 0:
            return [], []
        if isinstance(data, pd.DataFrame):
        # Take first column automatically
            data = data.iloc[:, 0]
        x = instance.ltpDf.index.to_numpy()
        
        # Convert to numeric if data is not already float or int
        if data.dtypes not in [np.float64, np.int64]:  
            y = pd.to_numeric(data, errors='coerce').to_numpy()
        else:
            y = data.to_numpy()

        # Ensure x and y have the same length
        min_len = min(len(x), len(y))
        x, y = x[:min_len], y[:min_len]  

        # Create mask and apply it safely
        mask = ~np.isnan(y)
        mask = mask[:min_len]  # Ensure mask has the same length as x and y

        return x[mask], y[mask]  # Filter out NaN values
                        
    def update():
        # update hmaps:
        #instance.hmap_buy_LH.setImage(instance.lowHighdf[0].to_numpy().T, autoLevels=False)
        #instance.hmap_buy_HL.setImage(instance.highLowdf[0].to_numpy().T, autoLevels=False)
        #instance.hmap_sell_LH.setImage(instance.lowHighdf[1].to_numpy().T, autoLevels=False)
        #instance.hmap_sell_HL.setImage(instance.highLowdf[1].to_numpy().T, autoLevels=False)
        instance.hmap_buy.setImage(instance.combineddf[0].to_numpy().T, autoLevels=False)
        instance.hmap_sell.setImage(instance.combineddf[1].to_numpy().T, autoLevels=False)

        min_ltp = instance.ltpDf['ltp'].min()
        n = len(instance.ltpDf)
        m = len(instance.aggDf)
        for h in [instance.hmap_buy,instance.hmap_sell]:
        #for h in [instance.hmap_buy_HL,instance.hmap_buy_LH,instance.hmap_sell_HL,instance.hmap_sell_LH]:
            h.setRect(0, min_ltp, n, m)

        # update line plots:
        x = instance.ltpDf.index.to_numpy()
        y_ltp = instance.ltpDf['ltp'].to_numpy()
        instance.line_ltp_buy.setData(x, y_ltp)
        instance.line_ltp_sell.setData(x, y_ltp)
        #update buy trend lines
        x, y = getXY(instance.lowHighMaxes[0])
        instance.line_upper_1_buy.setData(x, y)
        x, y = getXY(instance.HighlowMaxes[0])
        instance.line_lower_1_buy.setData(x, y)
        #update sell trend lines:
        x, y = getXY(instance.lowHighMaxes[0])
        instance.line_upper_1_sell.setData(x, y)
        x, y = getXY(instance.HighlowMaxes[0])
        instance.line_lower_1_sell.setData(x, y)
        #update voll_diff:
        ##
        
        x,y = getXY(instance.voldiff_buy[0])
        instance.vol_diff_50.setData(x, y)
        x,y = getXY(instance.voldiff_sell[0])
        instance.vol_diff_20.setData(x, y)

        x = instance.ltpDf.index.to_numpy()
        y = pd.to_numeric(instance.ltpDf['buy-vol'], errors='coerce')
        instance.buy_vol.setData(x, y)

        #y = pd.to_numeric(instance.ltpDf['sell-vol'], errors='coerce')
        #instance.sell_vol.setData(x, y)


        """
        x,y = getXY(instance.voldiff_sell0[0])
        instance.vol_diff_200.setData(x, y)
        x,y = getXY(instance.voldiff_300[0])
        instance.vol_diff_300.setData(x, y)
        """
        ##
    while instance.ltpDf.empty:
        time.sleep(1)


    app = pg.mkQApp()

    win_buy = pg.GraphicsLayoutWidget(title="Buy Heatmap & Trends")
    win_buy.setBackground("white")
    plot_buy = win_buy.addPlot(title="Buy Side")
    plot_buy.addLegend()
    win_buy.show()

    win_sell = pg.GraphicsLayoutWidget(title="Sell Heatmap & Trends")
    win_sell.setBackground("white")
    plot_sell = win_sell.addPlot(title="Sell Side")
    plot_sell.addLegend()
    win_sell.show()

    win_diff = pg.GraphicsLayoutWidget(title="Volume Difference")
    win_diff.setBackground("white")
    plot_diff = win_diff.addPlot(title="Volume Difference")
    plot_diff.addLegend()
    win_diff.show()


    # add heatmaps:
    #instance.hmap_buy_LH = addHeatMap(plot_buy, instance.lowHighdf[0].to_numpy().T)
    #instance.hmap_buy_HL = addHeatMap(plot_buy, instance.highLowdf[0].to_numpy().T)
    instance.hmap_buy = addHeatMap(plot_buy, instance.combineddf[0].to_numpy().T)

    instance.line_upper_1_buy = addlinePlot(plot_buy, linewidth=4, data=instance.lowHighMaxes[0], name='Buy Uptrend', color='#097969')
    instance.line_lower_1_buy = addlinePlot(plot_buy, linewidth=4, data=instance.HighlowMaxes[0], name='Buy Downtrend', color='#fbd604')

    #sell heatmaps:
    #instance.hmap_sell_LH = addHeatMap(plot_sell, instance.lowHighdf[1].to_numpy().T)
    #instance.hmap_sell_HL = addHeatMap(plot_sell, instance.highLowdf[1].to_numpy().T)
    instance.hmap_sell = addHeatMap(plot_sell, instance.combineddf[1].to_numpy().T)

    instance.line_upper_1_sell = addlinePlot(plot_sell, linewidth=4, data=instance.lowHighMaxes[1], name='Sell Uptrend', color='#5F9EA0')
    instance.line_lower_1_sell = addlinePlot(plot_sell, linewidth=4, data=instance.HighlowMaxes[1], name='Sell Downtrend', color='#fdd750')

    #voll_diff:
    instance.vol_diff_50 = addlinePlot(plot_diff, linewidth=4, data=instance.voldiff_buy, name='VolDiff_buy', color='#fdd750')
    instance.vol_diff_20 = addlinePlot(plot_diff, linewidth=4, data=instance.voldiff_sell, name='VolDiff_sell', color='#097969')
    #instance.vol_diff_300 = addlinePlot(plot_diff, linewidth=4, data=instance.voldiff_300, name='VolDiff_300', color=(0, 0, 255))

    # line plots
    instance.line_ltp_buy = addlinePlot(plot_buy, linewidth=2, data=instance.ltpDf['ltp'], name='LTP', color=(0, 0, 255))
    instance.line_ltp_sell = addlinePlot(plot_sell, linewidth=2, data=instance.ltpDf['ltp'], name='LTP', color=(0, 0, 255))

    #vols:
    instance.buy_vol = addlinePlot(plot_diff,linewidth=1,data=instance.ltpDf['buy-vol'],name='buyVol',color="#a0924a")
    #instance.sell_vol = addlinePlot(plot_diff,linewidth=1,data=instance.ltpDf['sell-vol'],name='sellVol',color="#0C3B34")


    # Adjust axis colors for visibility
    for p in [plot_buy, plot_sell, plot_diff]:
        p.getAxis("left").setPen(pg.mkPen("black"))
        p.getAxis("bottom").setPen(pg.mkPen("black"))
        p.showGrid(x=True, y=True)

    # Timer to update column count every second
    timer = pg.QtCore.QTimer()
    timer.timeout.connect(update)
    timer.start(50)  # Update every .1 seconds
    app.exec()




    
if __name__=='__main__':
    import matplotlib.pyplot as plt
    import numpy as np

    import matplotlib.animation as animation


    class PauseAnimation:
        def __init__(self):
            fig, ax = plt.subplots()
            ax.set_title('Click to pause/resume the animation')
            x = np.linspace(-0.1, 0.1, 1000)

            # Start with a normal distribution
            self.n0 = (1.0 / ((4 * np.pi * 2e-4 * 0.1) ** 0.5)
                    * np.exp(-x ** 2 / (4 * 2e-4 * 0.1)))
            self.p, = ax.plot(x, self.n0)

            self.animation = animation.FuncAnimation(
                fig, self.update, frames=200, interval=50, blit=True)
            self.paused = False

            fig.canvas.mpl_connect('button_press_event', self.toggle_pause)

        def toggle_pause(self, *args, **kwargs):
            if self.paused:
                self.animation.resume()
            else:
                self.animation.pause()
            self.paused = not self.paused

        def update(self, i):
            self.n0 += i / 100 % 5
            self.p.set_ydata(self.n0 % 20)
            return (self.p,)
    pa = PauseAnimation()
    plt.show()