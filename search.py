import redis
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (TimeoutException, 
                                      StaleElementReferenceException,
                                      NoSuchElementException)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains


redis_con = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )
keywords_queue = 'scrapy:keywords'
results_queue = 'scrapy:results'
suggestion_queue = 'scrapy:suggestion'

def get_item(driver, element_type, path, timeout=10, max_attempts=3):
    attempts = 0
    
    while attempts < max_attempts:
        try:
            if element_type == 1:  # Single element
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, path))
                )
                print("Element found!")
                return element
                
            elif element_type == 2:  # Multiple elements
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, path))
                )
                elements = driver.find_elements(By.XPATH, path)
                if elements:
                    print(f"Found {len(elements)} elements")
                    return elements
                raise NoSuchElementException("No elements found after wait")
                
        except (TimeoutException, NoSuchElementException) as e:
            print(f"Element(s) not found: {str(e)}")
            attempts += 1
            if attempts >= max_attempts:
                return None
                
        except StaleElementReferenceException:
            print("Element became stale, retrying...")
            attempts += 1
            if attempts >= max_attempts:
                return None
                
    return None

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.get("https://www.google.com")
try:
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//textarea[@id='APjFqb']"))
    )
    print("page loaded!")
except TimeoutException as e:
    print("Page doesn't loaded!")

search_input = get_item(driver, 1, "//textarea[@id='APjFqb']")
search_input.clear()
keyword = redis_con.brpop(keywords_queue, timeout=0)[1]
search_input.send_keys(keyword)

search_buttons = get_item(driver, 2, "//input[@name='btnK']")

print(search_buttons)

actions = ActionChains(driver)
actions.move_to_element(search_buttons[1])
actions.click(search_buttons[1])
actions.perform()

time.sleep(3)
results = get_item(driver, 1, "//div[@id='search']")
print(results)