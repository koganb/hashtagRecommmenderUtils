# using as example https://github.com/yanxurui/csv2es/blob/master/csv2es.py
import csv
import warnings

import click

# this code should be run one time, to create the network file and that's it (once the network.txt file exists -
# no need to rerun this code
warnings.filterwarnings("ignore", category=UnicodeWarning)


# convert to tab separated
def read(infile, outfile):
    with open(infile) as fin:
        reader = csv.reader(fin)
        reader.next()  # skip headers
        with open(outfile, 'w') as fout:
            for row in reader:
                line = "%s\t%s\n" % (row[0], row[1])
                fout.write(line)


@click.command()
@click.argument('infile', required=True)
@click.argument('outfile', required=True)
@click.pass_context
def cli(ctx, infile, outfile, **opts):
    read(infile, outfile)


if __name__ == '__main__':
    '''
    converts the 'friends_pairs' csv file to a txt file with 2 tab seperated columns wihtout a header

    parameters:
        argv[1]: file location of the network file (e.g.: "...TwitterProject\friends_pairs_small.csv")
        argv[2]: file location (+name) where to locate the new created file(e.g. - "...\TwitterProject\network.txt"
    '''
    cli()
