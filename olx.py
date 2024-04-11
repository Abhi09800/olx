import scrapy
import json
import csv
from scrapy.crawler import CrawlerProcess

class Olx(scrapy.Spider):
    name = 'olx'
    base_url = ('https://www.olx.in/api/relevance/v4/search?category=1723&facet_limit=100&lang=en-IN&location=4058877'
                '&location_facet_limit=20&page={}&platform=web-desktop&relaxedFilters=true&size=40'
                '&user=0061384243345810274')
    headers = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/123.0.0.0 Safari/537.36')
    }

    def __init__(self):
        super().__init__()
        self.items = []

    def start_requests(self):
        for page_number in range(1, 50):  
            yield scrapy.Request(url=self.base_url.format(page_number), headers=self.headers, callback=self.parse)

    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"Received non-200 response: {response.status}")
            return

        try:
            data = json.loads(response.body)
            for offer in data['data']:
                item = {
                    'Title': offer['title'],
                    'Description': offer['description'].replace('\n', ''),
                    'Location': offer['locations_resolved']['COUNTRY_name'] + ',' +
                                offer['locations_resolved']['ADMIN_LEVEL_1_name'] + ',' +
                                offer['locations_resolved']['ADMIN_LEVEL_3_name'] + ',' +
                                offer['locations_resolved']['SUBLOCALITY_LEVEL_1_name'],
                    'ID': offer['ad_id'],
                    'Price': offer['price']['value']['display'],
                    'Bed': offer['main_info'],
                    'Date': offer['display_date']
                }
                self.items.append(item)
        except Exception as e:
            print("Error while parsing response data:", e)
            print("Response body:", response.body)

        if not data.get('metadata', {}).get('has_next_page', False):
            self.write_to_csv()

    def write_to_csv(self):
        with open('result.csv', 'a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.items[0].keys())
            if csv_file.tell() == 0:  
                writer.writeheader()
            writer.writerows(self.items)
            self.items = []  

# Run scraper
process = CrawlerProcess()
process.crawl(Olx)
process.start()
