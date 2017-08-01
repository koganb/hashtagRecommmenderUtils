
#using as example https://github.com/yanxurui/csv2es/blob/master/csv2es.py
import json
import urllib2
from urllib2 import *
import click
import dateutil.parser
import warnings
from datetime import datetime
warnings.filterwarnings("ignore", category=UnicodeWarning)

def transform_json(line):
    j = json.loads(line)
    solr_dist = dict()
    solr_dist['id'] = j['tweet_id']
    solr_dist['userid'] = j['user_id']
    solr_dist['text'] = j['tweet_text'].encode('UTF-8', errors='ignore')
    #solr_dist['hashtags'] = [x.encode('ascii', errors='ignore') for x in j['hashtags']]
    solr_dist['hashtags'] = [x.encode('UTF-8', errors='ignore') for x in j['hashtags']]
    solr_dist['timestamp'] = int(time.mktime(dateutil.parser.parse(
        str(j['date'])).timetuple()))

    return json.dumps(solr_dist)


def upload (file, solr_host, solr_core, bulk_size):
    with open(file) as f:
        payload = list()
        for line in f:
            if len(line.strip()) > 0:
                payload.append(transform_json(line))
            if len(payload) == bulk_size:
                sendRequestsBulkToSolr(solr_host, solr_core, payload)
                payload = list()
        if len(payload)> 0:
            sendRequestsBulkToSolr(solr_host, solr_core, payload)


def sendRequestsBulkToSolr(solr_host, solr_core, payload):
    solr_url = solr_host + '/solr/' + solr_core + '/update/json?commit=true'
    print  solr_url
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
    upload(file + '_train.txt', ctx.obj['solr_host'], ctx.obj['core'] + '_train', ctx.obj['bulk_size'])
    upload(file + '_test.txt', ctx.obj['solr_host'], ctx.obj['core'] + '_test', ctx.obj['bulk_size'])

if __name__ == '__main__':
    start_time = datetime.now()
    print "twitter2solr has just started running"
    cli()
    duration = (datetime.now() - start_time).seconds
    print "\nThe whole process took us: " + str(duration / 60.0) + " minutes"
