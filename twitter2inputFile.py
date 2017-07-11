# using as example https://github.com/yanxurui/csv2es/blob/master/csv2es.py
import json
import urllib2
from urllib2 import *
import click
import dateutil.parser
import warnings
import random

warnings.filterwarnings("ignore", category=UnicodeWarning)


def transform_json(line):
    j = json.loads(line)
    tweet_id = j['tweet_id']
    user_id = j['user_id']
    hash_tags = ','.join([x.encode('ascii', errors='ignore') for x in j['hashtags']])
    timestamp = int(time.mktime(dateutil.parser.parse(
        str(j['date'])).timetuple()))

    return '"%s";"%s";"%s";"%s";""\n' % (user_id, tweet_id, timestamp, hash_tags)


def read(infile, outfile):
    train_file_name = outfile + "_train.txt"
    test_file_name = outfile + "_test.txt"
    train_test_file_name = outfile + ".txt"

    with open(infile) as fin:
        with open(train_file_name, 'w') as fout_train:
            with open(test_file_name, 'w') as fout_test:
                for line in fin:
                    if len(line.strip()) > 0:
                        line = transform_json(line)
                        if random.randint(0, 4) == 0:
                            fout_test.write(line)
                        else:
                            fout_train.write(line)

    import fileinput
    with open(train_test_file_name, 'w') as fout:
        fin = fileinput.input([train_file_name, test_file_name])
        for line in fin:
            fout.write(line)
        fin.close()



@click.command()
@click.argument('infile', required=True)
@click.argument('outfile', required=True)
@click.pass_context
def cli(ctx, infile, outfile, **opts):
    read(infile, outfile)


if __name__ == '__main__':
    cli()
