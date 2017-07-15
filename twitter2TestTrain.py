# using as example https://github.com/yanxurui/csv2es/blob/master/csv2es.py
import json
import urllib2
from urllib2 import *
import click
import dateutil.parser
import warnings
import random

warnings.filterwarnings("ignore", category=UnicodeWarning)


def read(infile, outfile):
    train_file_name = outfile + "_train.txt"
    test_file_name = outfile + "_test.txt"

    with open(infile) as fin:
        with open(train_file_name, 'w') as fout_train:
            with open(test_file_name, 'w') as fout_test:
                for line in fin:
                    if len(line.strip()) > 0:
                        if random.randint(0, 4) == 0:
                            fout_test.write(line)
                        else:
                            fout_train.write(line)


@click.command()
@click.argument('infile', required=True)
@click.argument('outfile', required=True)
@click.pass_context
def cli(ctx, infile, outfile, **opts):
    read(infile, outfile)


if __name__ == '__main__':
    cli()
