import requests
import json
import indices
import csv

# CC Index API endpoint
index_list = indices.get_commoncrawl_indexes()

# Query for *.mn URLs
params = {
    'url': '*.mn',
    'output': 'json'
}

for idx in index_list:
    api_url = f"https://index.commoncrawl.org/{idx}-index"
    response = requests.get(api_url, params=params)
    
    result = []
    for line in response.iter_lines():
        if line:
            record = json.loads(line)
            result.append(record['url'])

    with open(f"data/{idx}.csv", "w+") as file:
        wr = csv.writer(file, quoting=csv.QUOTE_ALL, delimiter='\n')
        wr.writerow(result)

    print(f"{len(result)} urls in {idx}.csv file.")
    break