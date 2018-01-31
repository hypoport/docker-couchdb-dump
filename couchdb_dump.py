#!/usr/bin/env python

import sys
import json
import argparse
import requests


def parse_args():
    parser = argparse.ArgumentParser(description='Dump couchdb documents as JSON')
    parser.add_argument('-H', '--host', dest='host', default='localhost', help='database host')
    parser.add_argument('-P', dest='port', type=int, default=5984, help='database port')
    parser.add_argument('-u', dest='username', help='database username')
    parser.add_argument('-p', dest='password', help='database password')
    parser.add_argument('database', help='the database to dump')
    return parser.parse_args()


def dump(host, port, database, username, password, page_size=5000):
    auth = (username, password or '') if username else None
    uri = 'http://{}:{:d}/{}/_all_docs'.format(host, port, database)

    session = requests.Session()
    response = session.get(uri, auth=auth, params={
        'include_docs': True,
        'limit': page_size + 1,
    })
    sys.stdout.write('{"docs":[\n')
    previous_doc = None
    while True:
        if response.status_code == 200:
            response_json = response.json()
            next_doc = None
            rows = response_json['rows']
            for index, row in list(enumerate(rows)):
                doc = row['doc']
                if index == page_size:
                    next_doc = doc
                    break
                else:
                    next_doc = None
                if previous_doc:
                    sys.stdout.write(',\n')
                del doc['_rev']
                sys.stdout.write(json.dumps(doc, separators=(',', ':')))
                previous_doc = doc
            if next_doc:
                response = session.get(uri, auth=auth, params={
                    'include_docs': True,
                    'startkey_docid': next_doc['_id'],
                    'limit': page_size + 1,
                })
            else:
                break
        else:
            sys.stderr.write("Response status: {:d}".format(response.status_code))
            sys.exit(1)
            break
    sys.stdout.write(']}\n')


ARGS = parse_args()
dump(ARGS.host, ARGS.port, ARGS.database, ARGS.username, ARGS.password)
