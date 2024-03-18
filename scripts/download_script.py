from tqdm import tqdm
import pickle
from stock_chart_cnn import CoinbaseWrapper

def testing():
    """
    Function to perform testing.

    This function sets up the necessary parameters for testing and generates chart images.
    """
    # presets for testing
    ticker = 'btcusd'
    bartime = '4_hour'
    
    coinbase = CoinbaseWrapper()
    
    price = coinbase.get_current_price(ticker)
    print(f'Current price for {ticker}: {price}')
    
    #2018-01-01 in timestamp UTC
    start = 1514764800
    # 2018-01-30 in timestamp UTC
    end = 1517270400
    data = coinbase.get_data_in_date_range(ticker, bartime, start, end, save=False)
    print(data)
    
    
def running():
    """
    Function to generate chart images for crypto tickers and time intervals.
    
    This function reads in the arrays for tickers and time intervals, and then iterates over each ticker and time interval
    combination to generate chart images using the specified parameters. The generated images are saved in the specified
    directory.
    """
    # presets for running
    parentdir = 'crypto'
    
    # read in the arrays for tickers
    crypto_tickers = pickle.load(open('data/crypto/iterables/coinbase_tickers.pkl', 'rb'))
    time_intervals = pickle.load(open('data/crypto/iterables/time_intervals.pkl', 'rb'))
    
    coinbase = CoinbaseWrapper()
    
    # custom tickers and range
    crypto_tickers = ['btcusd', 'ethusd']
    time_intervals = ['15_minute', '5_minute']
    
    #######################################
    # Find way to get ealiest listed data on coinbase
    #######################################
    
    
    #2018-01-01 in timestamp UTC
    start = 1514764800
    # 2024-03-01 in timestamp UTC
    end = 1709251200
    
    for ticker in tqdm(crypto_tickers, desc='Crypto: '):
        for bartime in tqdm(time_intervals, desc='Bartime: '):
            data = coinbase.get_data_in_date_range(ticker, bartime, start, end, save=True)

def main():
    testing()
    #running()
    
# run script
if __name__ == '__main__':
    main()