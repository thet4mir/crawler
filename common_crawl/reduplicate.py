import csv
from os import listdir
from os.path import isfile, join
from urllib.parse import urlparse


def get_domain(url):
    """Extracts the domain from a URL (e.g., 'example.mn' from 'https://sub.example.mn/path')"""
    parsed = urlparse(url)
    if parsed.netloc == '':
        return None
    # Remove port number if present
    domain = parsed.netloc.split(':')[0]
    return domain

file_list = [f for f in listdir('data') if isfile(join(f"data/{f}"))]


domains = set()

for file in file_list:
    with open(f"data/{file}", "r") as f:
        csv_file = csv.reader(f)
        for line in csv_file:
            domains.add(get_domain(line[0]))

with open("domain.csv", "w") as f:
    wr = csv.writer(f, quoting=csv.QUOTE_ALL, delimiter='\n')
    wr.writerow(domains)