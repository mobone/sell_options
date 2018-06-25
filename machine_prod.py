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
from sklearn.externals import joblib

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
        sleep(15)
        print('starting')
        client = pymongo.MongoClient(ip+':27017',
                                     username = username,
                                     password = password,
                                     authSource='finance')
        db = client.finance
        collection = db.machine_results_2

        self.data_df = pd.read_csv("results.csv").ix[:,1:]
        del self.data_df['Expired']
        self.q = RedisQueue('features_input', host=ip)
        self.out_q = RedisQueue('machine_output', host=ip)
        self.max_profit = 0
        while self.q.qsize():
            failed = False
            self.selected_features = eval(str(self.q.get())[2:-1])

            self.df = self.data_df.copy()
            self.select_data()
            output = {'Trades Before': [], 'Trades After': [], 'Predicted': [], 'Predicted_cutoff': []}
            for i in range(self.selected_features['exp_count']):
                self.split()
                target = self.selected_features['targets'][0]
                self.train(target)
                self.test(target)

                self.y_test['Predicted '+target+' Rank'] = self.y_test['Predicted '+target].rank()
                self.y_test['Total Rank'] = self.y_test['Predicted '+target+' Rank']
                self.y_test = self.y_test.sort_values(by='Total Rank')
                if self.y_test.tail(self.selected_features['trade_count'])['Profit'].mean()<50:
                    failed = True
                    break

                output['Trades After'].extend(self.y_test.tail(self.selected_features['trade_count'])['Profit'])
                output['Trades Before'].extend(self.y_test['Profit'])
                output['Predicted'].extend(self.y_test.tail(self.selected_features['trade_count'])['Predicted '+target])
                output['Predicted_cutoff'].append(float(self.y_test.tail(self.selected_features['trade_count'])['Predicted '+target].min()))
            if failed==True:
                continue
            if len(output['Trades After'])==len(output['Trades Before']):
                continue
            # create results
            output_df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in output.items() ]))
            del output_df['Predicted_cutoff']
            output_df = output_df.describe().T[::-1]

            output_df.loc['Change'] = output_df.ix[:2,:].pct_change().ix['Trades After',:]

            if output_df.ix['Change', 'mean']>0.50 and output_df['mean']['Trades After']>125.0:
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
                res['value']['Cutoff'] = sum(output['Predicted_cutoff'])/len(output['Predicted_cutoff'])

                self.selected_features['Cutoff'] = res['value']['Cutoff']
                res['value']['strike_num'] = self.selected_features['strike_num']
                res['value']['Underlying_Price'] = self.selected_features['Underlying_Price']
                before_sum = (sum(output['Trades Before']) - (6*len(output['Trades Before'])))/float(self.selected_features['exp_count'])
                after_sum = (sum(output['Trades After']) - (6*len(output['Trades After'])))/float(self.selected_features['exp_count'])
                res['value']['Before sum'] = before_sum
                res['value']['After sum'] = after_sum
                res['value']['Alert_Level'] = self.selected_features['alert_level']

                self.post_id = collection.insert(res['value'])


                self.save_machine(output_df['mean']['Trades After'])

    def save_machine(self, mean_profit):
        with open('results/'+str(mean_profit)+'_'+str(self.post_id)+'_features.txt', 'w') as f:
            f.write(json.dumps(self.selected_features))

        clf = svm.SVR()
        clf.fit(self.df.ix[:, self.selected_features['features']], self.df.ix[:,self.selected_features['targets']].values)
        joblib.dump(clf, 'results/'+str(mean_profit)+'_'+str(self.post_id)+'_model.clf')


    def select_data(self):

        self.strike_num = self.selected_features['strike_num']
        self.stock_price = self.selected_features['Underlying_Price']
        self.alert_level = self.selected_features['alert_level']
        if self.strike_num is not None:
            self.df = self.df[self.df['Strike Num']==float(self.strike_num)]
        if self.stock_price is not None:
            self.df = self.df[self.df['Underlying_Price']<float(self.stock_price)]
        self.df = self.df[self.df['Alert Level']>=self.alert_level]
        self.df = self.df[self.selected_features['features']+self.selected_features['targets']]
        self.df = self.df.dropna()
        self.total_df = self.df.copy()

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
    if 'Underlying_Price' not in k_best_features:
        k_best_features.append('Underlying_Price')
    if 'Alert Level' not in k_best_features:
        k_best_features.append('Alert Level')
    print(k_best_features)
    input()
    col_selection = []
    #col_selection.extend(list(itertools.combinations(k_best_features, 3)))
    #col_selection.extend(list(itertools.combinations(k_best_features, 4)))
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
                    for alert_level in [.01,.02]:
                        for stock_price in [25,50,75,None]:
                            selected_dict = {'features': list(selected),
                                             'targets': ['Profit'],
                                             'exp_count': 4,
                                             'trade_count': trade_count,
                                             'strike_num': strike_num,
                                             'Underlying_Price': stock_price,
                                             'alert_level': alert_level}
                        #q.put(selected_dict)
                    output_selection.append(selected_dict)
        print(len(output_selection))




    for i in range(1):
        x = machine()
        x.start()
        sleep(.1)
    shuffle(output_selection)
    for i in output_selection:
        q.put(i)
    max_profit = 0
    print(q.qsize())

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
