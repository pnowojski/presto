# -*- coding: utf-8 -*-
####################################################
# Sample script that shows how to save result data #
####################################################
import datetime
import os
import urllib
import urllib2
import argparse
import csv
import json
import random

# You need to enter the real URL and have the server running
CODESPEED_URL = 'http://localhost:8000/'

current_date = datetime.datetime.today()


parser = argparse.ArgumentParser(description='Upload jmh benchmark csv results')
parser.add_argument('--commit', dest='commit', required=True,
                    help='md5')
parser.add_argument('--branch', dest='branch', required=True)
parser.add_argument('--module', dest='module', required=True,
                    help='presto module')
parser.add_argument('--environment', dest='environment', required=True)
parser.add_argument('--dry', dest='dry', action='store_true')

def readData(args):
    results = []
    path = "%s/jmh-result.csv" % args.module
    modificationDate = datetime.datetime.fromtimestamp(os.path.getmtime(path))
    #modificationDate = datetime.date(2016, 8, int(args.commit))

    with open(path) as csvFile:
        reader = csv.reader(csvFile, delimiter=",")
        lines = [line for line in reader]
        header = lines[0]
        params = sorted(filter(lambda s : s.startswith("Param"), header))
        paramIndexes = map(lambda param : header.index(param), params)
        benchmarkIndex = header.index("Benchmark")
        scoreIndex = header.index("Score")
        errorIndex = scoreIndex + 1

        for line in lines[1:]:
            name = line[benchmarkIndex].split(".")[-1]
            if len(paramIndexes) > 0:
                for paramIndex in paramIndexes:
                    name += "." + line[paramIndex]

            coef = 1.0
            #coef = random.uniform(0.99, 1.01)
            #if name.find("bigint") > 0:
            #    coef *= 0.8

            #if name.find("with_hash") > 0:
            #    coef *= 0.8

            results.append({
                'commitid': args.commit,
                'branch': args.branch,
                'project': 'Presto',
                'executable': 'Presto',
                'benchmark': name,
                'environment': args.environment,
                #'result_value': float(line[scoreIndex]),
                'result_value': float(line[scoreIndex]) * coef,

                'revision_date': str(modificationDate),
                'result_date': str(modificationDate),
                'std_dev': line[errorIndex],  # Optional. Default is blank
            })
    return results

def add(data):
    #params = urllib.urlencode(data)
    response = "None"
    try:
        f = urllib2.urlopen(
            CODESPEED_URL + 'result/add/json/', urllib.urlencode(data))
    except urllib2.HTTPError as e:
        print str(e)
        print e.read()
        return
    response = f.read()
    f.close()
    print "Server (%s) response: %s\n" % (CODESPEED_URL, response)

if __name__ == "__main__":
    args = parser.parse_args()

    data = json.dumps(readData(args), indent=4, sort_keys=True)
    if args.dry:
        print data
    else:
        add({'json': data})
