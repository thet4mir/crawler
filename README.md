### common_crawler - extrect mongolian text and urls from Common crawl dataset
```
check common_crawler/README.md
```

### crawler - scrapy project can scrape any website resursively. However there are some limitation such as depth = 3 and max scraping page = 1000, for safety.
```
cd crawler/crawler/spiders
touch domain.csv
add the urls that you want to scrape in domain.csv file.
scrapy runspider recursive.py
```