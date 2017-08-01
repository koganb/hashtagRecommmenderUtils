# using as example https://github.com/yanxurui/csv2es/blob/master/csv2es.py
# splitting the data into train/test population for the hashtag project.
# 3 files are being created/modified here - twitter_sample.txt, twitter_sample_train.txt, twitter_sample_test.txt
import json
import pandas as pd
from urllib2 import *
import dateutil.parser
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", category=UnicodeWarning)


def seed_users_split(seed_users_data):
    '''
    handles the splitting to train/test of the seed users (from these users only the test set will be created)
    :param seed_users_data: pandas data-frame
        data-frame containing the seed users twitter data (this is a conversion of the json lines to a pandas df)
    :return: data frames
        2 data frames of train, test population data
    '''
    data_filtered = seed_users_data.groupby(['user_id']).filter(lambda group: len(group) > 1)
    data_filtered.reset_index(drop=True, inplace=True)
    print "\n{} rows were removed from the seed_users dataset due to the fact that these users had only a single instance".\
        format(seed_users_data.shape[0]-data_filtered.shape[0])
    test_idx = data_filtered.groupby(['user_id']).apply(lambda x: pd.to_datetime(x['date']).idxmax())
    train_idx = list(set(data_filtered.index) - set(test_idx))
    train_data = data_filtered.iloc[train_idx]
    test_data = data_filtered.iloc[test_idx]
    return train_data, test_data


def transform_single_line(j):
    '''
    converts a long row json object to a shrinked one with only the needed information (data needed in the java project)
    :param j: json object
        single json object containing full information of a single line
    :return: tuple
        tuple with only the 4 parmas needed for the java code (user_id, tweed_id, timestamp, hashtags)
    '''
    tweet_id = j['tweet_id']
    user_id = j['user_id']
    #hash_tags = ','.join([x.encode('ascii', errors='ignore') for x in j['hashtags']])
    hash_tags = ','.join([x.encode('UTF-8', errors='ignore') for x in j['hashtags']])
    timestamp = int(time.mktime(dateutil.parser.parse(
        str(j['date'])).timetuple()))

    return '"%s";"%s";"%s";"%s";""\n' % (user_id, tweet_id, timestamp, hash_tags)


def transform_full_data(data_as_df, timeline_file, java_file):
    '''
    writes into 2 files the needed information taken from a pandas object
    First write is to the timeline_file (this will hold all twitter data)
    Second write is to the java_file (this will hold only 5 central params needed in the java project)
    :param data_as_df: pandas data-frame
        the data-frame to convert into the 2 files
    :param timeline_file: string
        excat location+name of the timeline file, where full data will be written to
    :param java_file: string
        excat location+name of the java file, where partial data will be written to
    :return:
        nothing
    '''
    with open(timeline_file, 'a') as t_file, open(java_file, 'a') as j_file:
        for index, row in data_as_df.iterrows():
            t_file.write('\n')
            json.dump(row.to_dict(), t_file)
            line = transform_single_line(row)
            j_file.write(line)


def split_to_train_test(timeline_file, network_file, results_location):
    '''
    The code that actually generates the 4 differnt files,based on train/test split logic

    :param timeline_file: string
        exact location of the input file(+file name), which contains twitter full data ("timeline_results.csv")
    :param network_file: string
        exact location of the network file(+file name). This is the csv file with column headers ("friends_pairs.csv)
    :param results_location: string
        location folder (only folder, without the explicit name) where to locate the new files created
    :return:
        nothing
    '''
    # creating explicit names of the output files
    timeline_train = results_location + "\\timeline_results_train.txt"
    timeline_test = results_location + "\\timeline_results_test.txt"
    java_data_train = results_location + "\\data_for_java_train.txt"
    java_data_test = results_location + "\\data_for_java_test.txt"
    network_table = pd.read_csv(network_file)
    # pulling out the values of seed users and friends
    seed_users = set(network_table["seed_user"].unique())
    friend_users = set(network_table["friend"].unique())
    # this object will contatin list of dictionaries of all the seed users, later will be ocnverted to a pandas df
    seed_users_list = []
    with open(timeline_file) as fin:
        with open(timeline_train, 'a') as t_train, open(java_data_train, 'a') as java_train:
            # going over each row in the input data file
            for line in fin:
                j = json.loads(line)
                user_id = j['user_id']
                # in case the user not a seed one, but rather appears in the friends list
                if user_id in friend_users:
                    # writing data to the timeline_train
                    t_train.write('\n')
                    json.dump(j, t_train)
                    # writing data to the java_data_train
                    java_json = transform_single_line(j=j)
                    java_train.write(java_json)
                # in case the user is a seed user-we'll just add him to the data of the seed user and later will handle
                elif user_id in seed_users:
                    seed_users_list.append(j)

    # converting the seed users data from a list of dictionaries to a pandas data-frame
    seed_users_df = pd.DataFrame(seed_users_list)
    # splitting seed users to train/test (special handling here, because these are seed users)
    seed_users_df_train, seed_users_df_test = seed_users_split(seed_users_data=seed_users_df)
    # converting the dataframe into 2 files we need (doing it twice - for the train and test)
    transform_full_data(data_as_df=seed_users_df_train, timeline_file=timeline_train, java_file=java_data_train)
    transform_full_data(data_as_df=seed_users_df_test, timeline_file=timeline_test, java_file=java_data_test)

if __name__ == '__main__':
    '''
    splits the data into train/test population and created 4 different files for further usage.
    Files are: timeline_results_train.txt, timeline_results_test.txt, data_for_java_train.txt, data_for_java_test.txt

    parameters:
        argv[1]: exact location of the input file(+file name), which contains twitter full data.
                 Example : "...\FINAL_LISTS\timeline_results_boris.txt"
        argv[2]: location folder (only folder, without the explicit name) where to locate the new files created
                 Example: "...\pyhton_code\hashtagRecommmenderUtils"
        argv[3]: exact location of the network file(+file name). This is the csv file with column headers
                 Example: "...\FINAL_LISTS\friends_pairs.csv"

    '''
    infile = sys.argv[1]
    outfile = sys.argv[2]
    network_file = sys.argv[3]
    print "twitter2TestTrain has just started running"
    start_time = datetime.now()
    split_to_train_test(timeline_file=infile, results_location=outfile, network_file=network_file)
    duration = (datetime.now() - start_time).seconds
    print "\nThe whole process took us: " + str(duration / 60.0) + " minutes"

    # unused code
    '''
    train_df, test_df = timeline2pandas(infile, network_file)
    print "\nOver train dataset we have {} rows and {} distinct users." \
          "Over test dataset we have {} rows and {} distinct users".\
        format(train_df.shape[0], len(train_df.groupby(['user_id'])), test_df.shape[0], len(test_df.groupby(['user_id'])))
    duration = (datetime.now() - start_time).seconds
    print "\nTime it took us for the 1st phase is: " + str(duration / 60.0) + " minutes"
    start_time = datetime.now()
    transform_full_data(data_as_df=train_df, outfile=train_out)
    transform_full_data(data_as_df=test_df, outfile=test_out)
    duration = (datetime.now() - start_time).seconds
    print "\nTime it took us for the 2nd phase is: " + str(duration / 60.0) + " minutes"

    # writing the train+test data to a file (combination of the 2 train and test)
    start_time = datetime.now()
    with open(outfile + ".txt", 'w') as outfile:
        for fname in [train_out, test_out]:
            with open(fname) as infile:
                outfile.write(infile.read())
    duration = (datetime.now() - start_time).seconds
    print "\nTime it took us for the 3rd and last phase is: " + str(duration / 60.0) + " minutes"
    '''
