import requests
import pandas as pd
import logging
from datetime import datetime, timezone
from pathlib import Path

class CoinbaseWrapper(object):
    def __init__(self):
        """
        Initializes a new instance of the CoinbaseWrapper class.

        Args:
            ticker (str): The ticker symbol for the cryptocurrency.

        Returns:
            None
        """
        self._init_logger()
        self.bartimes_convert = {'1_min': 60, '5_minute': 300, '15_minute': 900, 
                                    '1_hour': 3600, '6_hour': 21600, '1_day': 86400}
        self.ticker = ticker.upper().replace('USD','-USD')

    def _init_logger(self):
        """
        Initializes the logger for the CoinbaseWrapper class.

        This method sets up a logger object with a stream handler and a specific formatter.
        The logger is then configured to log messages at the DEBUG level.

        Args:
            None

        Returns:
            None
        """
        self._logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.DEBUG)

    @staticmethod
    def _filepath_maker(ticker: str, bartime: str):
        filepath = 'data/crypto/formatted/{t}/{t}_{b}_data_formatted.csv'.format(t=ticker, b=bartime)
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        return filepath
    
    def get_4hour_data(self, ticker: str, start: int):
        """
        Fetches 4-hour candlestick data for the given currency from the Coinbase API.

        Args:
            start (int): The start time for the candlestick data in Unix timestamp format.

        Returns:
            pandas.DataFrame or None: A DataFrame containing the fetched candlestick data, or None if the request fails.
        """
        candles_df = self.fetch_data(ticker, '1_hour', start)
        if candles_df is not None:
            start, df = self.convert_4hour_data(candles_df)
            return start, df
        return None
    
    def convert_4hour_data(self, candles_df: pd.DataFrame):
        """
        Converts the given DataFrame of candlestick data into 4-hour candles.

        Args:
            candles_df (pd.DataFrame): The DataFrame containing candlestick data.

        Returns:
            pd.DataFrame: The DataFrame containing 4-hour candles with columns ['date', 'low', 'high', 'open', 'close', 'volume'].
        """
        start, end = None, None
        four_hour_candles = []
        for idx, row in candles_df.iterrows():
            if len(four_hour_candles) >= 75: # 300 hours of data max returned from API 4 * 75 = 300
                break # breaks out before it throws error for index not found
            if (row['date'].hour % 4 == 0) and end is None:
                end = idx+1
            elif (row['date'].hour % 4 == 0) and start is None:
                start = idx+1
                date = candles_df.at[end-1,'date']
                # minimum low value in period
                low = min(candles_df.loc[end:start,'low'].values)
                # maximum high value in period
                high = max(candles_df.loc[end:start,'high'].values)
                # the opening price at start
                open = candles_df.at[start-1,'open']
                # the closing price at end
                close = candles_df.at[end,'close']
                # the total volume
                volume = sum(candles_df.loc[end:start,'volume'].values)
                # add the 4 hour candle to the list
                four_hour_candles.append([date,low,high,open,close,volume])
                # reset index's
                start, end = None, start
        return pd.DataFrame(four_hour_candles, columns=['date','low','high','open','close','volume'])

    def fetch_data(self, ticker: str, bartime: str, start: int):
        if bartime == '4_hour':
            # get the 4 hour data
            candles_df = self.fetch_data(ticker, '1_hour', start)
            if candles_df is not None:
                return start, self.convert_4hour_data(candles_df)
            return None
        
        # convert the bartime to seconds
        bartime = self.bartimes_convert[bartime]
        # set the end time
        end = start + bartime * 300 # the limit of coinbase is 300 candles
        # format the ticker
        ticker = ticker.upper().replace('USD','-USD')
        
        url = 'https://api.pro.coinbase.com/products/{t}/candles'.format(t=ticker)
        
        response = requests.get(url, params={'start': start, 'end': end, 'granularity': bartime})
        
        if response.status_code == 200:
            candles = response.json()
            
            if candles is not None:
                # reverse the ordering
                candles.reverse()
                # create dataframe
                candles_df = pd.DataFrame(candles, columns=['date','low','high','open','close','volume'])
                # set the start of the next request
                start = candles_df.loc[-1, 'date']
                # convert the the timestamp to UTC (avoid overlapp with daylight savings time)
                convert_date = lambda x: datetime.fromtimestamp(x , timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                candles_df['date'] = candles_df['date'].apply(convert_date)
                candles_df.set_index('date', inplace=True)

                return start, candles_df
        return None, None
        
    def get_data_date_range(self, ticker, bartime, start, end):
        start, df = self.fetch_data(ticker, bartime, start)    
        # the start date or end date are invalid
        if start is None:
            self._logger.error(f"Failed to fetch {ticker} {bartime} candles data.")
            return None
        # loop through the data
        while start < end:
            start, temp = self.fetch_data(ticker, bartime, start)
            df = pd.concat([df, temp])
            # reached the end of the data
            if start is None:
                break
        # save the data to a csv file
        df.to_csv(self._filepath_maker(ticker, bartime))
    
    
    def fetch_last_n_candles(self, bartime, limit):
        """
        Fetches the last n candles for a given bartime and limit.

        Args:
            bartime (str): The bartime interval for the candles.
            limit (int): The number of candles to fetch.

        Returns:
            pandas.DataFrame or None: A DataFrame containing the fetched candles data, or None if the request fails.
        """
        # helper for 4_hour
        if bartime == '4_hour':
            candles_df = self.fetch_last_n_candles('1_hour', 40)
            if candles_df is not None:
                return self.convert_4hour_data(candles_df)
            return None # failed

        # get the request size
        bartime = self.bartimes_convert[bartime]
        
        # api endpoint for currency candles
        url = 'https://api.pro.coinbase.com/products/{t}/candles?granularity={b}&limit={l}'.format(t=self.ticker,
                                                                                                   b=bartime,
                                                                                                   l=limit)
        response = requests.get(url)
        
        if response.status_code == 200:
            candles = response.json()
            if candles is not None:
                candles.reverse() # oldest first
                candles_df = pd.DataFrame(candles, columns=['date','low','high','open','close','volume'])
                # convert date to datetime utc
                candles_df['date'] = candles_df['date'].apply(lambda x: datetime.utcfromtimestamp(x))
                return candles_df
        self._logger.error(f"Failed to fetch {self.ticker} candles data. Status code: {response.status_code}")
        return None
    
    def convert_4hour_data(self, candles_df: pd.DataFrame):
        """
        Converts the given DataFrame of candlestick data into 4-hour candles.

        Args:
            candles_df (pd.DataFrame): The DataFrame containing candlestick data.

        Returns:
            pd.DataFrame: The DataFrame containing 4-hour candles with columns ['date', 'low', 'high', 'open', 'close', 'volume'].
        """
        start, end = None, None
        four_hour_candles = []
        for idx, row in candles_df.iterrows():
            if len(four_hour_candles) >= 75: # 300 hours of data max returned from API 4 * 75 = 300
                break # breaks out before it throws error for index not found
            if (row['date'].hour % 4 == 0) and end is None:
                end = idx+1
            elif (row['date'].hour % 4 == 0) and start is None:
                start = idx+1

                date = candles_df.at[end-1,'date']
                low = min(candles_df.iloc[end:start]['low'].values) # minimum low value in period
                high = max(candles_df.iloc[end:start]['high'].values) # maximum high value in period
                open = candles_df.at[start-1,'open'] # the opening price at start
                close = candles_df.at[end,'close'] # the closing price at end
                volume = sum(candles_df.iloc[end:start]['volume'].values) # the total volume
                four_hour_candles.append([date,low,high,open,close,volume])

                # reset index's
                start, end = None, start
        #four_hour_candles.reverse()
        return pd.DataFrame(four_hour_candles, columns=['date','low','high','open','close','volume'])

    def get_current_price(self):
        """
        Retrieves the current price of the given currency from the Coinbase API.

        Returns:
            float: The current price of the currency.
            None: If an error occurs during the API request.
        """
        # Coinbase API endpoint for currency price
        url = f"https://api.coinbase.com/v2/prices/{self.ticker}/spot"
        
        try:
            response = requests.get(url)
            data = response.json()
            
            # Extracting the current price of the given currency
            currency_price = data['data']['amount']
            
            return float(currency_price)
        
        except Exception as e:
            self._logger.error("Error occurred:", e)
            return None