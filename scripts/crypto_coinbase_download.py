import pickle
import pandas as pd
from pathlib import Path
import requests
from datetime import datetime, timezone

def fetch_data(ticker: str, bartime: str, start: int):
    bartimes_convert = {'1_day': 86400, '4_hour': 14400, '1_hour': 3600,
                        '15_minute': 900, '5_minute': 300, '1_minute': 60}
    bartime = bartimes_convert[bartime]
    
    end = start + bartime * 300 # the limit of coinbase is 300 candles
    
    ticker = ticker.upper().replace('USD','-USD')
    
    url = 'https://api.pro.coinbase.com/products/{t}/candles'.format(t=ticker)
    
    response = requests.get(url, params={'start': start, 'end': end, 'granularity': bartime})
    
    if response.status_code == 200:
        candles = response.json()
        
        if candles is not None:
            candles.reverse()
            
            candles_df = pd.DataFrame(candles, columns=['date','low','high','open','close','volume'])
            
            start = candles_df['date'].iloc[-1]
            # convert the the timestamp to UTC (avoid overlapp with daylight savings time)
            convert_date = lambda x: datetime.fromtimestamp(x , timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            candles_df['date'] = candles_df['date'].apply(convert_date)
            candles_df.set_index('date', inplace=True)

            return start, candles_df
    return None, None

def filepath_maker(ticker: str, bartime: str):
    filepath = 'data/crypto/formatted/{t}/{t}_{b}_data_formatted.csv'.format(t=ticker, b=bartime)
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    return filepath


def main():
    # read in the arrays for tickers
    crypto_tickers = pickle.load(open('data/crypto/iterables/coinbase_tickers.pkl', 'rb'))
    time_intervals = pickle.load(open('data/crypto/iterables/time_intervals.pkl', 'rb'))
    
    crypto_tickers = ['ethusd', 'solusd'] 
    time_intervals = ['15_minute','5_minute']

    for ticker in crypto_tickers:
        for bartime in time_intervals:
            filepath = filepath_maker(ticker, bartime)
            
            start = 1514764800 #2018-01-01 in timestamp UTC
            # for solana
            if ticker == 'solusd':
                start = 1624406400
            end = 1709251200 # 2024-03-01 in timestamp UTC
            
            start, df = fetch_data(ticker, bartime, start)
            
            # the start date or end date are invalid
            if start == None:
                print('Invalid start or end date')
                break
            
            while start < end:
                start, temp = fetch_data(ticker, bartime, start)
                df = pd.concat([df, temp])
                df.to_csv(filepath)

if __name__=='__main__':
    main()