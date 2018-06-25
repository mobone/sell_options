import gc
import pandas_datareader.data as web
from datetime import datetime, timedelta
import pymongo
import pickle
import pandas as pd
import warnings
import queue
import threading
import random
from multiprocessing import Process, Queue
from random import shuffle
import configparser
from time import sleep
config = configparser.ConfigParser()
config.read('config.cfg')
username = config['creds']['User']
password = config['creds']['Pass']
ip = config['conn']['ip']
warnings.filterwarnings('ignore')
class backtest(Process):
    def __init__(self, q, out_q, low_price, high_price, stop_loss, profit_taking, alert_level):
        Process.__init__(self)
        self.q = q
        self.out_q = out_q
        self.low_price = low_price
        self.high_price = high_price
        self.stop_loss = stop_loss
        self.profit_taking = profit_taking
        self.alert_level = alert_level
        #self.strike_distance = strike_distance


    def run(self):
        self.output_columns = ['SMA20', 'SMA50', 'SMA200', 'Perf Week',
                               'Perf Month', 'Perf Quarter', 'Volatility_Week',
                               'Volatility_Month', 'Recom', 'RSI (14)', 'Rel Volume',
                               '52W High', '52W Low', 'Bid', 'Ask', 'Strike Distance',
                               'Days left', 'Expired', 'Strike Num', 'Underlying_Price',
                               'Market Cap', 'Alert Level', 'Profit', 'Max Loss']
        client = pymongo.MongoClient(ip+':27017',
                                     username = username,
                                     password = password,
                                     authSource='finance')

        db = client.finance
        self.options_coll = db.options_2
        self.finviz_coll = db.finviz
        while self.q.qsize():
            symbol = self.q.get()
            self.symbol = symbol
            try:
                self.option_symbol = None
                #print(self.symbol)
                self.get_start_end_dates()
                try:
                    self.get_historical_data()
                except Exception as e:
                    #print('err getting historical ', e)
                    continue
                self.iterate_through_history()
            except Exception as e:
                print('error:', e)
                continue
        print('complete')


    def get_start_end_dates(self):
        self.start_date = '20171031'
        self.start_date = '20171117'
        self.end_date = '20180501'

    def get_historical_data(self):
        self.historical = web.DataReader(self.symbol, 'morningstar', self.start_date, self.end_date)
        self.historical = self.historical.reset_index()

    def iterate_through_history(self):
        for self.date_index in range(1,len(self.historical)):
            prev_close =  self.historical.iloc[self.date_index-1]['Close']
            todays_date = str(self.historical.iloc[self.date_index]['Date']).replace('-','').split(' ')[0]
            previous_date = str(self.historical.iloc[self.date_index-1]['Date']).replace('-','').split(' ')[0]
            query = {'Root': self.symbol, 'iteration': 7, 'Update_Date': todays_date, 'Type': 'p'}
            #print(query)

            try:
                price = self.options_coll.find_one(query,{'Underlying_Price': 1})['Underlying_Price']
            except Exception as e:
                continue

            alert_level = (price-prev_close)/prev_close
            #print(todays_date, alert_level, self.alert_level)
            #input()
            self.stock_alert_level = alert_level
            if alert_level>self.alert_level and alert_level<.8:
                self.get_finviz(query.copy(), previous_date)

                self.get_todays_option(query, price, todays_date, alert_level)

    def get_finviz(self, query, previous_date):
        query['Date'] = int(previous_date)
        del query['Update_Date']
        del query['iteration']
        del query['Type']

        finviz_df = pd.DataFrame(self.finviz_coll.find_one(query), index=[0])

        for i in list(finviz_df.columns):
            if i not in self.output_columns:
                del finviz_df[i]
        if 'Market Cap' in finviz_df.columns:
            market_cap_value = finviz_df['Market Cap'].values[0]
            if 'K' in market_cap_value:
                market_cap_value = float(market_cap_value[:-1])*1000
            elif 'M' in market_cap_value:
                market_cap_value = float(market_cap_value[:-1])*1000000
            elif 'B' in market_cap_value:
                market_cap_value = float(market_cap_value[:-1])*1000000000

            finviz_df['Market Cap'] = market_cap_value

        self.finviz_df = finviz_df



    def get_option_results(self):
        for i in self.selected_option.iterrows():
            row, selected_option = i
            #print(self.selected_option)
            option_symbol = selected_option['Symbol']
            expiry_date = str(selected_option['Expiry']).replace('-','').split(' ')[0]
            start_date = str(self.historical.iloc[self.date_index]['Date']).replace('-','').split(' ')[0]
            query = {'Symbol': option_symbol,
                      'Update_Date': {
                            '$lte': expiry_date,
                            '$gt': start_date
                            }
                        }
            #print('2:',query)

            option_history = pd.DataFrame(list(self.options_coll.find(query)))
            #print(option_history.head())
            #print('got result')
            try:
                option_history = option_history.sort_values(by=['Update_Date', 'iteration'])
            except Exception as e:
                #print(e)
                selected_option = None
                option_symbol = None
                return
            #print(option_history)
            #input()
            buy_price = selected_option['Bid']
            sell_price = None

            max_loss = option_history['Ask'].max()
            #print(option_history.tail(3).head(1)['Update_Date'].values[0], option_history.tail(3).head(1)['Expiry'].values[0])
            #print(option_history.tail(3).head(1)['Underlying_Price'].values[0],option_history.tail(3).head(1)['Strike'].values[0])
            if option_history.tail(3).head(1)['Update_Date'].values[0]==option_history.tail(3).head(1)['Expiry'].values[0] and option_history.tail(3).head(1)['Underlying_Price'].values[0]<option_history.tail(3).head(1)['Strike'].values[0]:
                sell_price = 0.00
                selected_option['Expired'] = True
            else:
                final_option = option_history.ix[:,['Update_Date', 'Expiry', 'iteration', 'Bid', 'Ask']].tail(3).head(1).squeeze()
                sell_price = final_option['Ask']
                selected_option['Expired'] = False

            selected_option['Buy Price'] = buy_price
            selected_option['Sell Price'] = sell_price
            selected_option['Return Percent'] = ((sell_price-buy_price)/buy_price)*-1
            selected_option['Alert Level'] = self.stock_alert_level
            selected_option['Profit'] = ((buy_price - sell_price)*100)-6
            selected_option['Max Loss'] = float(max_loss)

            selected_option['Days left'] = int(str(selected_option['Expiry'] - selected_option['Update_Date']).split(' ')[0])

            # todo days till expiration
            for i in self.finviz_df.columns:
                selected_option[i] = self.finviz_df[i].values[0]
            #print(selected_option)
            selected_option = selected_option[self.output_columns]
            self.out_q.put(selected_option)


    def get_todays_option(self, query, price, todays_date, alert_level):
        option_start_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=30)).strftime('%Y%m%d')
        option_end_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=50)).strftime('%Y%m%d')
        query['Strike'] = {'$lt': price}
        query['Expiry'] = {'$gte': option_start_date, '$lte': option_end_date}
        #print('3:',query)
        #input()

        options = pd.DataFrame(list(self.options_coll.find(query)))

        #print(len(options))
        if len(options) == 0:
            return
        # TODO: incorporate into query
        options = options[(options['Bid']>=self.low_price) & (options['Bid']<=self.high_price)]
        options = options[(options['Ask']>=self.low_price) & (options['Ask']<=self.high_price)]
        options['Strike Distance'] = 1 - (options['Strike'] / options['Underlying_Price'])
        #options = options[options['Strike Distance']>self.strike_distance]
        if len(options) == 0:
            return
        options = options.sort_values(by='Strike')
        #print(options)
        #input()
        options['Expiry'] = pd.to_datetime(options['Expiry'])

        option = options.head(5)
        option = options.sort_values(by='Strike', ascending=False)
        option['Strike Num'] = option['Strike'].rank(method='dense')
        option['Update_Date'] = pd.to_datetime(option['Update_Date'])


        #self.option_symbol = option['Symbol'].values[0]
        self.selected_option = option

        self.get_option_results()

def check_if_running(processes):
    for i in processes:
        if i.is_alive():
            return True
    return False

if __name__ == '__main__':

    client = pymongo.MongoClient(ip+':27017',
                                 username = username,
                                 password = password,
                                 authSource='finance')

    db = client.finance
    options_coll = db.options_2
    q = Queue()
    out_q = Queue()

    symbols = list(options_coll.find({},{'Root': 1}).distinct('Root'))
    print(symbols)
    #symbols = ['BBY']
    #shuffle(symbols)

    for symbol in symbols:
        q.put(symbol)
    low_price = .8
    high_price = 2
    stop_loss = 350000
    profit_taking = .1
    alert_level = .01
    #strike_distance = .10
    processes = []

    for i in range(6):
        x = backtest(q, out_q,low_price, high_price, stop_loss, profit_taking, alert_level)
        x.start()
        #x.run()
        sleep(1)
        processes.append(x)

    #x = backtest(q, out_q,low_price, high_price, stop_loss, profit_taking, alert_level)
    #x.run()
    results = []
    histories = []
    start_qsize = q.qsize()
    while q.qsize() or check_if_running(processes):
        try:
            result = out_q.get(timeout=1)
        except:
            continue


        results.append(result.T)
        #histories.append(history)
        df = pd.DataFrame(results)

        #history_df = pd.DataFrame(histories)
        #history_df = history_df.T
        #history_df['Avg'] = history_df.mean(axis=1)
        #print(history_df.T)


        #print(pd.DataFrame([metrtics], columns = ['Total Profit', 'Pos Alert Total', 'Pos Alert Mean', 'Neg Alert Mean', 'Pos Alert Max Loss', 'Neg Alert Max Loss', 'Pos Count', 'Neg Count']))
        print(df.describe().ix[:,['Profit','Max Loss']])
        print(df.corr().ix[:,['Profit','Max Loss']])
        print(df['Profit'].sum())
        this_qsize = q.qsize()
        try:
            print((this_qsize-start_qsize)/start_qsize)
        except:
            pass
        try:
            df.to_csv('results.csv')
        except:
            print('close file!')
        #history_df.to_csv('history_results.csv')
