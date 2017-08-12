import sys

if __name__ == '__main__':
    '''
    joining the train and test into one big file. Doing it both to the time-line data and the java data
    '''

    files_location = sys.argv[1]
    timeline = files_location + "\\timeline_results"
    java_data = files_location + "\\data_for_java"
    # creating the full files (train+test)
    with open(timeline+".txt", 'a') as t:
        with open(timeline+"_train.txt", 'r') as t_train, open(timeline+"_test.txt", 'r') as t_test:
            t.write(t_train.read())
            t.write(t_test.read())
    with open(java_data+".txt", 'a') as j:
        with open(java_data+"_train.txt", 'r') as j_train, open(java_data+"_test.txt", 'r') as j_test:
            j.write(j_train.read())
            j.write(j_test.read())


# analysing the data in terms of hashtags
import pandas as pd
import numpy as np
import collections
import operator
import seaborn as sns
import matplotlib.pyplot as plt

data = pd.read_csv("C:\\Users\\abrahami\\Documents\\Private\\Uni\\BGU\\Reco.Systems - Bracha\\Project\\pyhton_code\\hashtagRecommmenderUtils\\data_for_java.txt",sep=";", header=None)
hashtags = data.iloc[:,3]
hashtags_splitted = [str(i).split(',') for i in hashtags]
flat_hashtags = [item for sublist in hashtags_splitted for item in sublist]
len(set(flat_hashtags))
hashtags_dict = collections.Counter(flat_hashtags)
sorted_hashtags = sorted(hashtags_dict.items(), key=operator.itemgetter(1))
newBins=np.array([1,2,3,4,5,6,7,8,9,10,12,14,16,18,20,25,30,40,50,75,100,150,200])
axes = plt.gca()
axes.set_xlim([150, 20000])
fig = sns.kdeplot(np.array(hashtags_dict.values()), shade=True, color="b", legend=True)
fig.figure.suptitle("Hashtags Counter Distribution", fontsize = 24)
plt.xlabel('Hashtag Counter', fontsize=18)
plt.ylabel('Density', fontsize=16)