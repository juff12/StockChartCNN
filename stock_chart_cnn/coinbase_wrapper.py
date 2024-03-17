import requests
import pandas as pd
import logging
from datetime import datetime, timezone
from pathlib import Path

class CoinbaseWrapper(object):
    def __init__(self):
        """
        Initializes a new instance of the CoinbaseWrapper class.

        Returns:
            None
        """
        self._init_logger()
        self.bartimes_convert = {'1_min': 60, '5_minute': 300, '15_minute': 900, 
                                 '1_hour': 3600, '6_hour': 21600, '1_day': 86400}

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
    
    def _get_4hour_data(self, ticker: str, start: int):
        """
        Fetches 4-hour candlestick data for the given currency from the Coinbase API.

        Args:
            start (int): The start time for the candlestick data in Unix timestamp format.

        Returns:
            pandas.DataFrame or None: A DataFrame containing the fetched candlestick data, or None if the request fails.
        """
        start, candles_df = self._fetch_data(ticker, '1_hour', start)
        if candles_df is not None:
            df = self._convert_4hour_data(candles_df)
            return start, df
        return None
    
    @staticmethod
    def _convert_4hour_data(candles_df: pd.DataFrame):
        """
        Converts the given DataFrame of candlestick data into 4-hour candles.

        Args:
            candles_df (pd.DataFrame): The DataFrame containing candlestick data.

        Returns:
            pd.DataFrame: The DataFrame containing 4-hour candles with columns ['date', 'low', 'high', 'open', 'close', 'volume'].
        """
        # flip the indexing for traversing
        candles_df.reset_index(inplace=True)

        # start and end index for 4 hour candles
        start, end = None, None
        four_hour_candles = []
        for idx, row in candles_df.iterrows():
            temp_date = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
            
            if len(four_hour_candles) >= 75: # 300 hours of data max returned from API 4 * 75 = 300
                break # breaks out before it throws error for index not found
            if (temp_date.hour % 4 == 0) and end is None:
                end = idx+1
            elif (temp_date.hour % 4 == 0) and start is None:
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
        
        # create a new dataframe and set the index
        four_hour_df = pd.DataFrame(four_hour_candles, columns=['date','low','high','open','close','volume'])
        four_hour_df.set_index('date', inplace=True)
        return four_hour_df

    def _fetch_data(self, ticker: str, bartime: str, start: int):
        """
        Fetches candlestick data from the Coinbase API. Gets the next 300 candles for the given ticker and bartime.

        Args:
            ticker (str): The ticker symbol for the cryptocurrency, expects form ex: btcusd.
            bartime (str): The bartime interval for the candles, accepted forms (1_day, 4_hour, 1_hour, 15_minute, 5_minute)
            start (int): The start timestamp for the data.

        Returns:
            tuple: A tuple containing the start timestamp for the next request and a DataFrame
                    containing the candlestick data. If the request fails or the data is empty,
                    None is returned.
        """
        if bartime == '4_hour':
            # get the 4 hour data
            return self._get_4hour_data(ticker, start)
        
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
                start = candles_df['date'].iloc[-1]
                # convert the the timestamp to UTC (avoid overlapp with daylight savings time)
                convert_date = lambda x: datetime.fromtimestamp(x , timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                candles_df['date'] = candles_df['date'].apply(convert_date)
                candles_df.set_index('date', inplace=True)

                return start, candles_df
        return None, None
        
    def get_data_in_date_range(self, ticker, bartime, start, end, save=False):
        start, df = self._fetch_data(ticker, bartime, start)    
        # the start date or end date are invalid
        if start is None:
            self._logger.error(f"Failed to fetch {ticker} {bartime} candles data.")
            return None
        # loop through the data
        while start < end:
            start, temp = self._fetch_data(ticker, bartime, start)
            df = pd.concat([df, temp])
            # reached the end of the data
            if start is None:
                break
        # save the data to a csv file
        if save:
            df.to_csv(self._filepath_maker(ticker, bartime))
        return df
    
    def get_current_price(self, ticker: str):
        """
        Retrieves the current price of the given currency from the Coinbase API.
        
        Args:
            ticker (str): The ticker symbol for the cryptocurrency, expects form ex: btcusd.
        
        Returns:
            float: The current price of the currency.
            None: If an error occurs during the API request.
        """
        # format the ticker
        ticker = ticker.upper().replace('USD','-USD')
        
        # Coinbase API endpoint for currency price
        url = "https://api.coinbase.com/v2/prices/{t}/spot".format(t=ticker)
        
        response = requests.get(url)
        
        if response.status_code == 200:
            price = response.json()
            
            if price is not None:
                price = price['data']['amount']
                
                return float(price)
        self._logger.error(f"Failed to fetch {ticker} current price.")
        return None