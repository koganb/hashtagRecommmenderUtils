
#using as example https://github.com/yanxurui/csv2es/blob/master/csv2es.py
import json
import urllib2
from urllib2 import *
import click
import dateutil.parser
import warnings
warnings.filterwarnings("ignore", category=UnicodeWarning)

def transform_json(line):
    j = json.loads(line)
    solr_dist = dict()
    solr_dist['id'] = j['tweet_id']
    solr_dist['user_id'] = j['user_id']
    solr_dist['text'] = j['tweet_text'].encode('ascii', errors='ignore')
    solr_dist['hashtags'] = [x.encode('ascii', errors='ignore') for x in j['hashtags']]
    solr_dist['timestamp'] = int(time.mktime(dateutil.parser.parse(
        str(j['date'])).timetuple()))

    return json.dumps(solr_dist)


def read (file, config):
    with open(file) as f:
        payload = list()
        for line in f:
            if len(line.strip()) > 0:
                payload.append(transform_json(line))
            if len(payload) == config['bulk_size']:
                sendRequestsBulkToSolr(config, payload)
                payload = list()
        if len(payload)> 0:
            sendRequestsBulkToSolr(config, payload)


def sendRequestsBulkToSolr(config, payload):
    solr_url = config['solr_host'] + '/solr/' + config['core'] + '/update/json?commit=true'
    req = urllib2.Request(url=solr_url, data="[" + ",".join(payload) + "]")
    req.add_header('Content-type', 'application/json')
    f = urllib2.urlopen(req)
    print f.read()


@click.command()
@click.argument('file', required=True)
@click.option('--solr-host', default='http://localhost:8983')
@click.option('--bulk-size', default=5000, help='How many docs to collect before writing to ElasticSearch')
@click.option('--core', help='Destination index name', default='twitter')
@click.pass_context
def cli(ctx, file, **opts):
    ctx.obj = opts
    read(file, ctx.obj)

if __name__ == '__main__':
    cli()