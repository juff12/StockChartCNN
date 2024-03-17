import pandas as pd
from lightweight_charts import Chart
from pathlib import Path

class ChartImage:
    def __init__(self, filename, ticker: str, bartime: str, chart_type: str, parentdir: str,
                 barspacing=1.5, num_bars_show=120, num_bar_gen=20, chart_width=1200, chart_height=600):
        self._df = self.read_csv(filename)
        # the name of the ticker
        self._ticker = ticker
        # the bartimes of the chart
        self._bartime = bartime
        # the type of chart (candle, line)
        self._chart_type = chart_type
        # the parent directory (crypto, stocks)
        self._parentdir = parentdir
        # barspacing, the space between the bars on chart (default 1.5)
        # bar spacing presets -> fits max zoom -> lower value = more zoom out, higher value = less zoom out
        self._barspacing = barspacing
        # the number of bars to show on the chart
        self._num_bars_show = num_bars_show
        # the amount of new bars to generate before a screenshot
        self._num_bar_gen = num_bar_gen
        # set the chart width
        self._chart_width = chart_width
        # set the chart height
        self._chart_height = chart_height
        
    def read_csv(self, filename: str):
        try:
            df = pd.read_csv(filename)
            # set the columns to the correct order and values
            df = df[['date','open','high','low','close']]
            return df
        except Exception:
            print('File does not exist or formating is wrong')
            return None
    
    def save_screenshot(self, image: list, start: int, end: int):
        # gen bar units
        _, unit = self._bartime.split('_')
        # create the filename
        filename = '{t}_{b}_{c}_{bs}_{nbs}_{nbg}_{u}_{s}_to_{u}_{e}'.format(t=self._ticker,b=self._bartime,
                                                                            c=self._chart_type,u=unit,s=str(start),
                                                                            e=str(end),bs=str(self._barspacing),
                                                                            nbs=str(self._num_bars_show),nbg=str(self._num_bar_gen))
        # root directory for the file
        file_root = 'inputs/raw/{p}/images/{t}/{c}/{b}/{f}_screenshot.png'.format(p=self._parentdir,t=self._ticker,
                                                                                  c=self._chart_type,b=self._bartime,f=filename)
        # create the file path
        filepath = Path(file_root)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        # save the image to the file
        with open(filepath, "wb") as out:
            out.write(image)
    
    @staticmethod
    def _take_screenshot(self, chart: Chart, start: int, end: int):
        # add the first set of data to the chart
        chart.set(self._df.iloc[start:end])
        # fit bars to the chart
        chart.fit()
        # display the chart
        chart.show()
        # exit the chart
        chart.exit()
        # take a screenshot of the chart
        return chart.screenshot()
                
    def sample_screenshots(self):
        chart = Chart(self._chart_width, self._chart_height)
        chart.crosshair('hidden')
        chart.grid(False,False)
        # starting and ending index for the starting state
        start, end = 0, self._num_bars_show
        image1 = self._take_screenshot(chart, start, end)
        # take a screenshot of the chart at after incrementing
        start, end = self._num_bar_gen, end + self._num_bars_show
        image2 = chart.screenshot()
        return image1, image2
    
    def create_chart_images(self, start=0, end=0):
        chart = Chart(self._chart_width, self._chart_height)
        chart.crosshair('hidden')
        chart.grid(False,False)
        # starting and ending index for the starting state
        start, end = 0, self._num_bars_show
        # add the first set of data to the chart
        chart.set(self._df.iloc[start:end])
        # fit bars to the chart
        chart.fit()
        # display the chart
        chart.show()
        
        # take a screenshot of the chart at starting state
        image = chart.screenshot()
        # save the screenshot
        self.save_screenshot(image, start, end)
        
        # intialize variables to keep track of updates
        count, start, idx = 1, self._num_bar_gen, end
        
        # loop through the data and take screenshots
        while idx < self.df['date'].size:
            # update the chart with the next set of data
            chart.update(self._df.iloc[idx])
            # if we have generated enough bars or we are at the end of the data
            if count == self._num_bar_gen or idx + 1 == self._df['date'].size:
                image = chart.screenshot()
                self.save_screenshot(image, start, idx)
                # reset the count
                count = 1
                # increment the index
                idx += 1
                # increment the start index
                start += self._num_bar_gen
                
                # reset the chart prevent lagging
                if count > 10000:
                    chart.exit()
                    self.create_chart_images(start, idx)
                    break
                continue
            # increment the index
            idx += 1
            # increment the count
            count += 1
        # will crash if chart is not exited
        chart.exit() # exit the chart to save memory