import pandas as pd
from sklearn import svm
from sklearn.model_selection import train_test_split
import itertools
from sklearn.preprocessing import normalize
from random import shuffle
from redis_queue_class import RedisQueue
from multiprocessing import Process
import configparser
import pymongo
from time import sleep
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression, mutual_info_regression
config = configparser.ConfigParser()
config.read('config.cfg')
username = config['creds']['User']
password = config['creds']['Pass']
ip = config['conn']['ip']
import json
class machine(Process):
    def __init__(self):
        Process.__init__(self)

    def run(self):
        client = pymongo.MongoClient(ip+':27017',
                                     username = username,
                                     password = password,
                                     authSource='finance')
        db = client.finance
        collection = db.machine_results

        self.data_df = pd.read_csv("results.csv").ix[:,1:]
        del self.data_df['Expired']
        self.q = RedisQueue('features_input', host=ip)
        self.out_q = RedisQueue('machine_output', host=ip)
        self.max_profit = 0
        while self.q.qsize():
            failed = False
            self.selected_features = eval(str(self.q.get())[2:-1])
            self.strike_num = self.selected_features['strike_num']
            self.stock_price = self.selected_features['stock_price']
            self.select_data()

            output = {'Trades Before': [], 'Trades After': [], 'Predicted': []}
            for i in range(self.selected_features['exp_count']):
                self.split()

                target = self.selected_features['targets'][0]
                self.train(target)
                self.test(target)

                self.y_test['Predicted '+target+' Rank'] = self.y_test['Predicted '+target].rank()
                self.y_test['Total Rank'] = self.y_test['Predicted '+target+' Rank']
                self.y_test = self.y_test.sort_values(by='Total Rank')
                if self.y_test.tail(200)['Profit'].mean()<50:
                    failed = True
                    break
                output['Trades After'].extend(self.y_test.tail(200)['Profit'])
                output['Trades Before'].extend(self.y_test['Profit'])
                output['Predicted'].extend(self.y_test.tail(200)['Predicted '+target])
            if failed==True:
                continue

            # create results
            output_df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in output.items() ]))
            output_df = output_df.describe().T[::-1]

            output_df.loc['Change'] = output_df.ix[:2,:].pct_change().ix['Trades After',:]

            if output_df.ix['Change', 'mean']>0.50:
                #print(output_df)
                profit = output_df['mean']['Trades After']
                if profit>self.max_profit:
                    self.max_profit = profit
                    print(output_df)
                res = pd.melt(output_df.reset_index(), id_vars=['index'])
                res.index = res['index']+' '+res['variable']
                del res['variable']
                del res['index']
                res = json.loads(res.to_json())

                res['value']['Features'] = self.selected_features['features']
                res['value']['Trade Count'] = self.selected_features['trade_count']

                before_sum = (sum(output['Trades Before']) - (6*len(output['Trades Before'])))/float(self.selected_features['exp_count'])
                after_sum = (sum(output['Trades After']) - (6*len(output['Trades After'])))/float(self.selected_features['exp_count'])
                res['value']['Before sum'] = before_sum
                res['value']['After sum'] = after_sum

                collection.insert(res['value'])

    def select_data(self):
        self.df = self.data_df[self.selected_features['features']+self.selected_features['targets']].dropna()
        if self.strike_num is not None:
            self.df = self.df[self.df['Strike Num']==self.strike_num]
        if self.stock_price is not None:
            self.df = self.df[self.df['Underlying_Price']<self.stock_price]

    def split(self):

        #input()
        self.X_train, self.X_test, self.y_train, self.y_test = \
            train_test_split(self.df.ix[:,self.selected_features['features']], \
            self.df.ix[:,self.selected_features['targets']])

    def train(self, target):
        self.clf = svm.SVR()
        self.clf.fit(self.X_train, self.y_train[target])

    def test(self, target):
        self.y_test['Predicted '+target] = self.clf.predict(self.X_test)

if __name__ == '__main__':
    df = pd.read_csv('results.csv').dropna().ix[:,1:]
    print(df.columns)
    del df['Expired']
    # choose k best features
    k = SelectKBest(f_regression, k=15)
    k = k.fit(df.ix[:,:-2],df['Profit'])

    k_best_features = list(df.ix[:, :-2].columns[k.get_support()])
    print(k_best_features)
    col_selection = []
    col_selection.extend(list(itertools.combinations(k_best_features, 3)))
    col_selection.extend(list(itertools.combinations(k_best_features, 4)))
    col_selection.extend(list(itertools.combinations(k_best_features, 5)))
    col_selection.extend(list(itertools.combinations(k_best_features, 6)))
    col_selection.extend(list(itertools.combinations(k_best_features, 7)))
    col_selection.extend(list(itertools.combinations(k_best_features, 8)))


    q = RedisQueue('features_input', host=ip)
    out_q = RedisQueue('machine_output', host=ip)
    q.remove()
    output_selection = []
    if q.qsize() == 0:
        print('loading queue')
        for selected in col_selection:
            for trade_count in [100,150,200]:
                for strike_num in [None, '1','2','3','4','5']:
                    for stock_price in [25,50,75,None]:
                        selected_dict = {'features': list(selected),
                                         'targets': ['Profit'],
                                         'exp_count': 4,
                                         'trade_count': trade_count,
                                         'strike_num': strike_num,
                                         'stock_price': stock_price}
                        #q.put(selected_dict)
                        output_selection.append(selected_dict)


    shuffle(output_selection)
    for i in output_selection:
        q.put(i)
    max_profit = 0
    print(q.qsize())
    exit()
    for i in range(12):
        x = machine()
        x.start()

    total_qsize = q.qsize()
    while True:
        init_q = q.qsize()
        sleep(60)
        print(total_qsize, init_q-q.qsize())
        """
        result = eval(str(out_q.get())[2:-1])
        print('output', result)
        profit = result[0]['Trades After']['mean']
        if profit>max_profit:
            max_profit = profit
            print(result[0])
            print(result[1])
        """
