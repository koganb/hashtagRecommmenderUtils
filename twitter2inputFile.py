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


def transform(infile, outfile):
    with open(infile) as fin:
        with open(outfile, 'w') as fout:
            for line in fin:
                if len(line.strip()) > 0:
                    line = transform_json(line)
                    fout.write(line)

@click.command()
@click.argument('infile', required=True)
@click.argument('outfile', required=True)
@click.pass_context
def cli(ctx, infile, outfile, **opts):
    train_file = infile + "_train.txt"
    test_file = infile + "_test.txt"
    train_out = outfile + "_train.txt"
    test_out = outfile + "_test.txt"

    transform(train_file, train_out)
    transform(test_file, test_out)

    with open(outfile + ".txt", 'w') as outfile:
        for fname in [train_out, test_out]:
            with open(fname) as infile:
                outfile.write(infile.read())

if __name__ == '__main__':
    cli()
