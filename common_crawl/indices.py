import requests
from bs4 import BeautifulSoup

def get_commoncrawl_indexes():
    url = 'https://index.commoncrawl.org/collinfo.json'
    response = requests.get(url)
    response.raise_for_status()
    return [index['id'] for index in response.json()]


if __name__ == "__main__":
    cc_indexes = get_commoncrawl_indexes()