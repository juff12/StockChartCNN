import pickle


def main():

    coinbase_crypto_tickers = [
        "btcusd", "ethusd", "xrpusd", "bchusd", "ltcusd", "eosusd", "dashusd", "linkusd", "zrxusd", "etcusd",
        "xtzusd", "oxtusd", "repusd", "atomusd", "algousd", "dntusd", "batusd", "cvcusd", "manausd", "kncusd",
        "loomusd", "nuusd", "mkrusd", "nmrusd", "omgusd", "rlcusd", "snxusd", "compusd",
        "umausd", "yfiusd", "renusd", "balusd", "wbtcusd", "aaveusd", "filusd", "adausd", "grtusd",
        "solusd", "lrcusd", "cgldusd", "ctsiusd", "sklusd", "sushiusd", "ankrusd", "1inchusd", "enjusd",
        "nknusd", "ognusd", "chzusd", "rlyusd", "trbusd", "bondusd", "keepusd", "mlnusd","truusd", "forthusd", 
        "ghstusd", "maskusd", "audiousd", "gtcusd","mirusd", "axsusd", "lptusd", "flowusd", "farmusd"
    ]

    time_intervals = ['1_day','4_hour','1_hour', '15_minute', '5_minute']


    with open('data/crypto/iterables/coinbase_tickers.pkl', 'wb') as fp:
        pickle.dump(coinbase_crypto_tickers, fp)
        
    with open('data/crypto/iterables/time_intervals.pkl', 'wb') as fp:
        pickle.dump(time_intervals, fp)

if __name__=='__main__':
    main()