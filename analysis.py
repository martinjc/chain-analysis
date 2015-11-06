import csv
import json

from venue_match import get_min_venue_from_csv

csv_reader = csv.DictReader(open('min_venues.csv', 'r'))  #, 'utf-8'))
venues = {}
for i, v in enumerate(csv_reader):

    if i % 10000 == 0:
        print(i)

    venue = get_min_venue_from_csv(v)
    venues[venue['id']] = venue

with open('simple_chains_with_names.json', 'r') as in_file:
    chains = json.load(in_file)

    count = 0
    for c in chains:
        if len(c['venues']) > 30:
            print c['id']
            count += 1
            for v in c['venues']:
                print venues[v]['name'],

    print "\n"
    print len(chains)
    print count
