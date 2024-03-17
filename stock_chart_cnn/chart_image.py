import pandas as pd
from lightweight_charts import Chart
from pathlib import Path
import random

class ChartImage:
    def __init__(self, filename, ticker: str, bartime: str, chart_type: str, parentdir: str,
                    barspacing=1.5, num_bars_show=120, num_bar_gen=20, chart_width=1200, chart_height=600):
        """
        Initialize a ChartImage object.

        Args:
            filename (str): The path to the CSV file containing the chart data.
            ticker (str): The name of the ticker.
            bartime (str): The bartimes of the chart.
            chart_type (str): The type of chart (candle, line).
            parentdir (str): The parent directory (crypto, stocks).
            barspacing (float, optional): The space between the bars on the chart. Defaults to 1.5.
            num_bars_show (int, optional): The number of bars to show on the chart. Defaults to 120.
            num_bar_gen (int, optional): The amount of new bars to generate before a screenshot. Defaults to 20.
            chart_width (int, optional): The width of the chart. Defaults to 1200.
            chart_height (int, optional): The height of the chart. Defaults to 600.
        """
        self._df = self.read_csv(filename)
        self._ticker = ticker
        self._bartime = bartime
        self._chart_type = chart_type
        self._parentdir = parentdir
        self._barspacing = barspacing
        self._num_bars_show = num_bars_show
        self._num_bar_gen = num_bar_gen
        self._chart_width = chart_width
        self._chart_height = chart_height
        self._id = random.randint(0,100_000)

    def _setup_chart(self):
        """
        Sets up and returns a Chart object with specified width and height.
        
        Returns:
            chart (Chart): The initialized Chart object.
        """
        chart = Chart(self._chart_width, self._chart_height)
        chart.crosshair('hidden')
        chart.grid(False,False)
        return chart
    
    def read_csv(self, filename: str):
        """
        Reads a CSV file and returns the data as a pandas DataFrame.

        Parameters:
        filename (str): The path to the CSV file.

        Returns:
        pandas.DataFrame: The data read from the CSV file, with columns in the order ['date', 'open', 'high', 'low', 'close'].
        If the file does not exist or the formatting is incorrect, None is returned.
        """
        try:
            data = pd.read_csv(filename)
            # set the columns to the correct order and values
            data = data[['date','open','high','low','close']]
            return data
        except Exception:
            print('File does not exist or formatting is wrong')
            return None
    
    def save_screenshot(self, image: list):
        """
        Saves the screenshot image to a file.

        Args:
            image (list): The screenshot image data.
            start (int): The start index.
            end (int): The end index.
        """
        self._id += 1
        # gen bar units
        _, unit = self._bartime.split('_')
        # create the filename
        filename = '{t}_{b}_{c}_{bs}_{nbs}_{nbg}_{id}'.format(t=self._ticker,b=self._bartime,
                                                              c=self._chart_type,u=unit,bs=str(self._barspacing),
                                                              id=self._id, nbs=str(self._num_bars_show),
                                                              nbg=str(self._num_bar_gen))
        # root directory for the file
        file_root = 'inputs/raw/{p}/images/{t}/{c}/{b}/{f}_screenshot.png'.format(p=self._parentdir,t=self._ticker,
                                                                                  c=self._chart_type,b=self._bartime,f=filename)
        # create the file path
        filepath = Path(file_root)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        # save the image to the file
        with open(filepath, "wb") as out:
            out.write(image)
    
    def _take_screenshot(self, chart: Chart, start: int, end: int):
        """
        Takes a screenshot of the chart within the specified range.

        Args:
            chart (Chart): The chart object.
            start (int): The start index of the data to be added to the chart.
            end (int): The end index of the data to be added to the chart.

        Returns:
            bytes: The screenshot image data.
        """
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
        """
        Takes screenshots of the chart at different states. For testing purposes.

        Returns:
            tuple: A tuple containing two images of the chart.
        """
        # initialize the chart to the presets
        chart = self._setup_chart()
        # starting and ending index for the starting state
        start, end = 0, self._num_bars_show
        image1 = self._take_screenshot(chart, start, end)
        # take a screenshot of the chart after incrementing
        start, end = self._num_bar_gen, end + self._num_bars_show
        image2 = chart.screenshot(chart, start, end)
        return image1, image2
    
    def create_chart_images(self, data, start=0, end=None):
        """
        Creates and saves chart images based on the provided data.

        Args:
            data (pandas.DataFrame): The data used to generate the chart images.
            start (int, optional): The starting index of the data. Defaults to 0.
            end (int, optional): The ending index of the data. If not provided, it is set to self._num_bars_show.

        Returns:
            None
        """
        # initialize the chart to the presets
        chart = self._setup_chart()
        # starting and ending index for the starting state
        if end is None:
            end = self._num_bars_show
        
        # add the first set of data to the chart
        chart.set(data.iloc[start:end])
        # fit bars to the chart
        chart.fit()
        # display the chart
        chart.show()
        
        # take a screenshot of the chart at starting state
        image = chart.screenshot()
        # save the screenshot
        self.save_screenshot(image)
        
        # intialize variables to keep track of updates
        count, start, idx = 0, start + self._num_bar_gen, end
        
        
        
        # loop through the data and take screenshots
        while idx < data['date'].size:
            # update the chart with the next set of data
            chart.update(data.iloc[idx])
            # if we have generated enough bars or we are at the end of the data
            if count == self._num_bar_gen or idx + 1 == data['date'].size:
                image = chart.screenshot()
                self.save_screenshot(image)

                # reset the count
                count = 0
                # increment the index
                idx += 1
                # increment the start index
                start += self._num_bar_gen
                continue
            # increment the index
            idx += 1
            # increment the count
            count += 1
        chart.exit()
            
    def batch_screenshot(self, batch_size=10_000):
        """
        Takes screenshots of stock chart images in batches.

        Args:
            batch_size (int): The number of stock chart images to process in each batch.
        """
        # loop through the data and take screenshots for each batch
        for i in range(0, self._df['date'].size, batch_size):
            # create a batch of the data
            batch_df = self._df.iloc[i:i+batch_size].copy()
            if i + batch_size >= self._df['date'].size:
                batch_df = self._df.iloc[i:].copy()
            self.create_chart_images(batch_df)
        