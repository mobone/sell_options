import pandas as pd
from sklearn import svm
from sklearn.model_selection import train_test_split
import itertools
from sklearn.preprocessing import normalize
from random import shuffle

df = pd.read_csv('results.csv').dropna().ix[:,1:]

col_selection = []
col_selection.extend(list(itertools.combinations(df.columns[:-2], 3)))
col_selection.extend(list(itertools.combinations(df.columns[:-2], 4)))
col_selection.extend(list(itertools.combinations(df.columns[:-2], 5)))
col_selection.extend(list(itertools.combinations(df.columns[:-2], 6)))
col_selection.extend(list(itertools.combinations(df.columns[:-2], 7)))
col_selection.extend(list(itertools.combinations(df.columns[:-2], 8)))
shuffle(col_selection)
print(col_selection)
exit()
exp_count = 5
max_profit = 0
for col_selected in col_selection:
    final_describes = []
    col_selected = list(col_selected)
    #col_selected.append('Profit')
    for i in range(exp_count):
        #this_df = df.copy()

        #this_df = df[col_selected]

        #this_df.ix[:,col_selected[:-1]] = normalize(this_df.ix[:,col_selected[:-1]])

        X_train, X_test, y_train, y_test = train_test_split(df.ix[:,col_selected], df.ix[:,'Profit'], test_size = .6)

        clf = svm.SVR()
        clf.fit(X_train, y_train)

        predicted = clf.predict(X_test)
        output = pd.DataFrame([predicted,y_test]).T
        output.columns = ['Predicted', 'Actual']
        initial_describe = output.Actual.describe()
        #init_profit = (output.Actual - 6).sum()

        #output = output.sort_values(by='Predicted').tail(200)
        this_len = 1000
        mean_exp = 1
        while this_len>150:
            mean_exp += .001
            this_len = len(output[output['Predicted']>output['Predicted'].mean()*mean_exp])

        output = output[output['Predicted']>output['Predicted'].mean()*mean_exp]

        final_describe = output.Actual.describe()
        final_describe['mean_exp'] = mean_exp
        final_describes.append(final_describe)
        #final_profit = (output.Actual-6).sum()
        #print(initial_describe, final_describe)
        #print(pd.DataFrame([initial_describe, final_describe], index = ['Before', 'After']))
        #print(init_profit, final_profit)

    result = pd.DataFrame(final_describes)
    result = result.dropna()
    if len(result)!=exp_count:
        #print('not enough')
        #print(result)
        continue

    if result['mean'].mean()>max_profit:
        max_profit = result['mean'].mean()

        result.loc['Before'] = initial_describe.T
        print(result)
        print(col_selected, max_profit)
