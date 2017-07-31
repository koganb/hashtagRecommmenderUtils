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