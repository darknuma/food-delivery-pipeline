import csv
from selenium import webdriver
from pydantic import BaseModel
from selenium.webdriver.chrome.service import Service
from typing import List
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

url = "https://d7leadfinder.com/app/view-leads/24562159/"
tb = "table table-striped table-bordered compact hover dataTable no-footer"
next_page = "page-link"

class ScrapeModel(BaseModel):
    name: str

def init_scrape(driver):
    data_list = []
    try:
        table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, f".{tb.replace(' ', '.')}")))
        rows = table.find_elements(By.TAG_NAME, "tr")[1:]

        for row in rows:
            try:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 13:
                    data = ScrapeModel(name=cols[0].text.strip())
                    data_list.append(data)
            except Exception as e:
                logging.error(f"Error scraping row: {e}")
                continue
    except Exception as e:
        logging.error(f"Error scraping table: {e}")

    return data_list

def click_next_button(driver, button):
    driver.execute_script("arguments[0].click();", button)

def scrape_data(driver, max_pages=5) -> List[ScrapeModel]:
    all_data = []
    try:
        driver.get(url)
        logging.info("Page title: %s", driver.title) 
        time.sleep(5)

        page_counter = 0  # Counter for pages

        while page_counter < max_pages:
            page_data = init_scrape(driver)
            all_data.extend(page_data)

            try:
                next_buttons = driver.find_elements(By.CLASS_NAME, next_page)
                
                next_button = None
                for button in next_buttons:
                    if button.text.strip().lower() == "next":
                        next_button = button
                        break

                
                if next_button and "disabled" not in next_button.get_attribute("class"):
                    logging.info("Navigating to next page...")
                    click_next_button(driver, next_button)  # Use JavaScript to click
                    time.sleep(5)  
                    page_counter += 1 
                else:
                    logging.info("No more pages to scrape")
                    break
            except TimeoutException:
                logging.info("No more pages to scrape")
                break
            except Exception as e:
                logging.error(f"Error navigating to next page: {e}")
                break

        logging.info(f"Successfully scraped {len(all_data)} records from {page_counter} pages")
        return all_data
    except Exception as e:
        logging.error(f"Error in scrape_data: {e}")
        return []



def save_to_csv(data: List[ScrapeModel], file_name: str):
    """Saves the scraped data to a CSV file."""
    logging.info("Saving data to CSV file...")
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Name"])
        for record in data:
            writer.writerow([record.name])
    logging.info(f"Data saved to {file_name}")

if __name__ == "__main__":
    logging.info("Scraping restaurants")
    logging.info("Driver Initialized")
    driver = webdriver.Chrome()
    try:
        data = scrape_data(driver)
        save_to_csv(data, "scraped_data.csv")
    finally:
        driver.quit()
