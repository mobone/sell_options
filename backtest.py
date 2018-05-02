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
        #options = options_coll.find({'Root': self.symbol, 'iteration': 7}).distinct('Update_Date')
        #dates = pd.DataFrame(list(options))
        #dates = dates.sort_values(by=0)
        #self.start_date = dates.head(1).values[0][0]
        #self.end_date = dates.tail(1).values[0][0]
        self.start_date = '20171031'
        self.end_date = '20180501'

    def get_historical_data(self):
        self.historical = web.DataReader(self.symbol, 'morningstar', self.start_date, self.end_date)
        self.historical = self.historical.reset_index()

    def iterate_through_history(self):
        for self.date_index in range(1,len(self.historical)):
            prev_close =  self.historical.iloc[self.date_index-1]['Close']
            todays_date = str(self.historical.iloc[self.date_index]['Date']).replace('-','').split(' ')[0]

            query = {'Root': self.symbol, 'iteration': 7, 'Update_Date': todays_date, 'Type': 'p'}
            print(query)

            try:
                price = self.options_coll.find_one(query,{'Underlying_Price': 1})['Underlying_Price']
            except Exception as e:
                continue

            alert_level = (price-prev_close)/prev_close
            print(todays_date, alert_level, self.alert_level)
            #input()

            if alert_level>self.alert_level and alert_level<.8:
                self.get_finviz(query)
                self.get_todays_option(query, price, todays_date, alert_level)

    def get_finviz(self, query):
        query['Date'] = int(query['Update_Date'])
        del query['Update_Date']
        del query['iteration']
        del query['Type']

        print(query)
        self.finviz_data = self.options_coll.find_one(query)
        print(self.finviz_data)
        input()

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
            print(query)

            option_history = pd.DataFrame(list(self.options_coll.find(query)))

            try:
                option_history = option_history.sort_values(by=['Update_Date', 'iteration'])
            except Exception as e:
                print(e)
                selected_option = None
                option_symbol = None
                return
            print(option_history)
            #input()
            buy_price = selected_option['Bid']
            sell_price = None
            """
            for row in option_history.iterrows():
                row_df = row[1]

                if row_df['Ask']>=self.stop_loss:
                    sell_price = row_df['Ask']
                    #print(self.selected_option)
                    #print(row_df)
                    #exit()
                    break
                if row_df['Ask']<=self.profit_taking:
                    sell_price = row_df['Ask']
                    break


            if sell_price is None:
                final_option = option_history.ix[:,['Update_Date', 'Expiry', 'iteration', 'Bid', 'Ask']].tail(3).head(1).squeeze()
                sell_price = final_option['Ask']
            """
            max_loss = option_history['Ask'].max()
            if option_history.tail(3).head(1)['Update_Date']==option_history.tail(3).head(1)['Expiry'] and option_history.tail(3).head(1)['Underlying_Price']<option_history.tail(3).head(1)['Strike']:
                sell_price = 0.00
            else:
                final_option = option_history.ix[:,['Update_Date', 'Expiry', 'iteration', 'Bid', 'Ask']].tail(3).head(1).squeeze()
                sell_price = final_option['Ask']


            #option_history['Mid Point'] = (option_history['Bid'] + option_history['Ask'])/2.0

            """
            fifty_percent = option_history[option_history['Bid']<(buy_price/2.0)]
            if len(fifty_percent):
                self.selected_option['Fifty Percent'] = True
            else:
                self.selected_option['Fifty Percent'] = False
            """

            #final_option = option_history.ix[:,:].tail(3).head(1).squeeze()


            #print(len(option_history))

            #if final_option['Expiry'] == expiry_date and final_option['Strike']<final_option['Underlying_Price']:
            #    sell_price = 0.0
            #else:
            #    sell_price = final_option['Bid']
            selected_option['Buy Price'] = buy_price
            selected_option['Sell Price'] = sell_price
            selected_option['Return Percent'] = ((sell_price-buy_price)/buy_price)*-1
            selected_option['Profit'] = ((buy_price - sell_price)*100)-6
            selected_option['Max Loss'] = float(max_loss)
            #option_history.index = range(len(option_history), 0, -1)
            # todo days till expiration
            print(selected_option)
            input()
            self.out_q.put(selected_option)


    def get_todays_option(self, query, price, todays_date, alert_level):
        option_start_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=30)).strftime('%Y%m%d')
        option_end_date = (datetime.strptime(todays_date,'%Y%m%d')+timedelta(days=50)).strftime('%Y%m%d')
        query['Strike'] = {'$lt': price}
        query['Expiry'] = {'$gte': option_start_date, '$lte': option_end_date}
        print(query)
        #input()
        options = pd.DataFrame(list(self.options_coll.find(query)))
        print(options)
        print(len(options))
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
        print(options)
        #input()
        options['Expiry'] = pd.to_datetime(options['Expiry'])

        option = options.head(5)

        self.option_symbol = option['Symbol'].values[0]
        self.selected_option = option
        #self.selected_option = option.squeeze()
        #print(self.option_symbol)
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


    #symbols = list(options_coll.find({},{'Root': 1}).distinct('Root'))
    symbols = ['BBY']
    shuffle(symbols)

    for symbol in symbols:
        q.put(symbol)
    low_price = .8
    high_price = 2
    stop_loss = 350000
    profit_taking = .1
    alert_level = .01
    #strike_distance = .10
    processes = []
    """
    for i in range(15):

        x = backtest(q, out_q,low_price, high_price, stop_loss, profit_taking, alert_level, strike_distance)
        x.start()
        sleep(1)
        processes.append(x)
    """
    x = backtest(q, out_q,low_price, high_price, stop_loss, profit_taking, alert_level)
    x.run()
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

        metrtics = [df['Profit'].sum(),df[df['Alert Level']>0]['Profit'].sum(), \
              df[df['Alert Level']>0]['Profit'].mean(), \
              df[df['Alert Level']>0]['Profit'].min(), \
              len(df[df['Alert Level']>0]['Profit'])]
        #print(pd.DataFrame([metrtics], columns = ['Total Profit', 'Pos Alert Total', 'Pos Alert Mean', 'Neg Alert Mean', 'Pos Alert Max Loss', 'Neg Alert Max Loss', 'Pos Count', 'Neg Count']))
        print(df.describe())
        print(df.corr()['Profit'])
        print(df['Profit'].sum())
        this_qsize = q.qsize()
        print((this_qsize-start_qsize)/start_qsize)
        try:
            df.to_csv('results.csv')
        except:
            print('close file!')
        #history_df.to_csv('history_results.csv')
