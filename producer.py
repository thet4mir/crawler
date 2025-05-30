import redis
import csv

# Connect to Redis
r = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True
)

def add_keywords_to_queue():
    keywords = []
    with open("keywords.csv", "r") as file:
        csvfile = csv.reader(file)
        keywords = [item[0] for item in csvfile]
        print(keywords)    
    queue_name = 'scrapy:keywords'
    
    for keyword in keywords:
        r.lpush(queue_name, keyword)
        print(f"Added keyword to queue: {keyword}")
    
    print(f"Total keywords in queue: {r.llen(queue_name)}")

if __name__ == "__main__":
    add_keywords_to_queue()