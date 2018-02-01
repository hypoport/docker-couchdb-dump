#!/usr/bin/env python

import argparse
import sys
import json
from decimal import Decimal
import ijson
import requests


def parse_args():
    parser = argparse.ArgumentParser(description='Load JSON documents into a CouchDB database')
    parser.add_argument('-H', '--host', dest='host', default='localhost')
    parser.add_argument('-P', dest='port', type=int, default=5984)
    parser.add_argument('-u', dest='username')
    parser.add_argument('-p', dest='password')
    parser.add_argument('target_database', help='database to import data to')
    return parser.parse_args()


def default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)


def load(args, input_file, bulk_size=1):
    session = requests.session()
    bulk = []
    for doc in ijson.items(input_file, 'docs.item'):
        bulk.append(doc)
        if len(bulk) == bulk_size:
            bulk_insert(args, session, bulk)
            bulk = []
    if bulk:
        bulk_insert(args, session, bulk)


def bulk_insert(args, session, docs):
    url = 'http://{}:{:d}/{}/_bulk_docs'.format(args.host, args.port, args.target_database)
    auth = (args.username, args.password or '') if args.username else None
    payload = json.dumps({'docs': docs}, separators=(',', ':'), default=default)
    response = session.post(url, data=payload, auth=auth, headers={
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-Couch-Full-Commit': 'true'
    })
    if response.status_code != 201:
        sys.stderr.write('Failed to import documents into {} ({:d})\n'.format(
            args.target_database, response.status_code))
        sys.exit(1)


def main():
    args = parse_args()
    load(args, sys.stdin, bulk_size=25000)

main()
