import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.exceptions import CloseSpider
import csv
import os
from urllib.parse import urlparse


class RecursiveSpider(CrawlSpider):
    name = 'robust_recursive_spider'
    allowed_domains = []

    # Configure settings
    custom_settings = {
        'DEPTH_LIMIT': 3,
        'CLOSESPIDER_PAGECOUNT': 1000,
        'AUTOTHROTTLE_ENABLED': True,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 2,  # Retry failed requests twice
        'HTTPERROR_ALLOW_ALL': True,  # Handle all HTTP status codes
        'TEXT_OUTPUT_FILE': 'combined_text.txt',
        'SKIPPED_URLS_FILE': 'skipped_urls.csv',
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_TIMEOUT': 30,  # 30 seconds timeout
    }

    visited_urls = set()  # Global URL tracker
    text_data = []        # Accumulate text content
    skipped_urls = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Read URLs from CSV file
        self.start_urls = self.read_urls_from_csv('domain.csv')
        self.allowed_domains = self.get_domains()
        
        # Initialize link extractor once
        self.link_extractor = LinkExtractor(
            allow_domains=self.allowed_domains,
            deny=[r'\?', r'\.(pdf|docx|zip|jpg|png|gif)$', r'/tag/', r'/user/'],
            unique=True
        )
        
        self.logger.info(f"Starting with {len(self.start_urls)} URLs")
        self.logger.info(f"Allowed domains: {self.allowed_domains}")

    def get_domains(self):
        """Extract domains from start URLs for validation"""
        domains = set()
        for url in self.start_urls:
            parsed = urlparse(url)
            if parsed.netloc:
                domains.add(parsed.netloc)
        return list(domains)

    def read_urls_from_csv(self, filename):
        """Read URLs from CSV file with error handling"""
        urls = []
        if not os.path.exists(filename):
            self.logger.error(f"CSV file not found: {filename}")
            return urls
            
        try:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if row:  # Skip empty rows
                        url = row[0].strip()
                        if url and url not in self.visited_urls:
                            urls.append(url)
                            self.visited_urls.add(url)
            self.logger.info(f"Loaded {len(urls)} URLs from CSV")
        except Exception as e:
            self.logger.error(f"Error reading CSV: {str(e)}")
        return urls

    async def start(self):
        """Generate initial requests from CSV URLs"""
        for url in self.start_urls:
            self.allowed_domains.append(url)
            print(url)
            yield scrapy.Request(
                url, 
                callback=self.parse_page,
                errback=self.handle_error,
                meta={
                    'handle_httpstatus_all': True,
                    'depth': 1  # Start at depth 1
                }
            )

    def process_request(self, request, response):
        """Filter duplicate requests"""
        if request.url in self.visited_urls:
            self.logger.debug(f"Skipping duplicate: {request.url}")
            return None
        return request

    def parse_page(self, response):
        """Process responses and extract links/text"""
        # Track visited URL
        self.visited_urls.add(response.url)
        current_depth = response.meta.get('depth', 1)
        
        # Skip non-200 responses
        if response.status != 200:
            self.logger.warning(f"Skipping {response.url} (Status: {response.status})")
            self.skipped_urls.append({
                'url': response.url,
                'reason': f'HTTP {response.status}'
            })
            return
        
        # Extract and save text content
        text_lines = response.css('body *::text').getall()
        cleaned_text = ' '.join([
            line.strip() for line in text_lines 
            if line.strip() and len(line.strip()) > 1
        ])
        cleaned_text = ' '.join(cleaned_text.split())
        
        if cleaned_text:
            self.write_text_to_file(response.url, cleaned_text)
        else:
            self.logger.warning(f"No text extracted from: {response.url}")
            self.skipped_urls.append({
                'url': response.url,
                'reason': 'No text content'
            })
        
        # Find and follow links if within depth limit
        if current_depth < self.settings.getint('DEPTH_LIMIT', 3):
            self.logger.debug(f"Extracting links at depth {current_depth} from {response.url}")
            links = self.link_extractor.extract_links(response)
            
            for link in links:
                url = link.url
                if url not in self.visited_urls:
                    self.visited_urls.add(url)
                    self.logger.debug(f"Queueing link: {url} (depth {current_depth + 1})")
                    yield response.follow(
                        url,
                        callback=self.parse_page,
                        errback=self.handle_error,
                        meta={
                            'depth': current_depth + 1,
                            'handle_httpstatus_all': True
                        }
                    )
                else:
                    self.logger.debug(f"Skipping duplicate: {url}")


    def write_text_to_file(self, url, text):
        print(text)
        """Write extracted text to output file"""
        text_file = self.settings.get('TEXT_OUTPUT_FILE', 'combined_text.txt')
        with open(text_file, 'a', encoding='utf-8') as f:
            f.write(f"URL: {url}\n")
            f.write(text)
            f.write("\n\n" + "=" * 80 + "\n\n")


    def handle_error(self, failure):
        """Handle request failures gracefully"""
        url = failure.request.url
        self.visited_urls.add(url)  # Prevent retries
        
        # Classify error type
        if failure.check(scrapy.exceptions.TimeoutError):
            reason = "Timeout"
        elif failure.check(scrapy.exceptions.TCPTimedOutError):
            reason = "TCP Timeout"
        elif failure.check(scrapy.exceptions.DNSLookupError):
            reason = "DNS Failure"
        elif failure.check(scrapy.http.HttpError):
            reason = f"HTTP Error ({failure.value.response.status})"
        else:
            reason = str(failure.type)
        
        self.logger.warning(f"Failed: {url} - {reason}")
        self.skipped_urls.append({
            'url': url,
            'reason': reason
        })

    def closed(self, reason):
        """Final processing when spider closes"""
        # Export text content
        text_file = self.settings.get('TEXT_OUTPUT_FILE')
        with open(text_file, 'r+', encoding='utf-8') as f:
            for entry in self.text_data:
                f.write(entry['text'] + '\n\n')
        
        # Export skipped URLs report
        skipped_file = self.settings.get('SKIPPED_URLS_FILE')
        if self.skipped_urls:
            with open(skipped_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['url', 'reason'])
                writer.writeheader()
                writer.writerows(self.skipped_urls)
        
        # Final stats
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Total URLs processed: {len(self.visited_urls)}")
        self.logger.info(f"Successfully scraped: {len(self.text_data)}")
        self.logger.info(f"Skipped URLs: {len(self.skipped_urls)}")
        self.logger.info(f"Text output: {os.path.abspath(text_file)}")
        self.logger.info(f"Skipped URLs report: {os.path.abspath(skipped_file)}")