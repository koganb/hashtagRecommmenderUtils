# using as example https://github.com/yanxurui/csv2es/blob/master/csv2es.py
import csv
import warnings

import click

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
    cli()
