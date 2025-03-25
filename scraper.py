import os
import time
import random
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure Selenium WebDriver
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--headless")

# Start WebDriver
driver = webdriver.Chrome(options=options)

# Get the current script path
current_path = os.path.dirname(os.path.realpath(__file__))

# Input and output file paths
INPUT_FILE = os.path.join(current_path, 'Code.txt')
OUTPUT_FILE = os.path.join(current_path, 'Annual_Reports.xlsx')

# Load stock codes from txt file
def load_stock_codes_from_txt():
    """Load stock codes from txt file"""
    try:
        with open(INPUT_FILE, 'r') as file:
            stock_codes = [line.strip() for line in file.readlines()]
        return stock_codes
    except Exception as e:
        print(f"Error: Unable to read stock codes file - {str(e)}")
        exit()

# Visit page and retrieve stockId
def get_stock_ids(stock_codes):
    """Use Selenium to get stockId corresponding to stock codes"""
    results = []
    try:
        url = "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en"
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[4]/section/div/div/form/div/div/section/div[1]/div/div[2]/ul/li[3]/div/div[1]/div[1]/input"))
        )

        for code in stock_codes:
            try:
                search_input = driver.find_element(By.XPATH, "/html/body/div[4]/section/div/div/form/div/div/section/div[1]/div/div[2]/ul/li[3]/div/div[1]/div[1]/input")
                search_input.clear()
                search_input.send_keys(code)

                time.sleep(random.uniform(2, 5))

                try:
                    suggestion_element = driver.find_element(By.XPATH, "/html/body/div[4]/section/div/div/form/div/div/section/div[1]/div/div[2]/ul/li[3]/div/div[1]/div[2]/div[1]/div[1]/table/tbody/tr[1]")
                    suggestion_element.click()
                except Exception:
                    pass

                search_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "/html/body/div[4]/section/div/div/form/div/div/section/div[1]/div/div[3]/a[2]"))
                )

                search_button.click()
                time.sleep(random.uniform(2, 5))

                try:
                    stock_id_element = driver.find_element(By.ID, "stockId")
                    stock_id_value = stock_id_element.get_attribute("value")
                    print(f"Stock code {code} stockId retrieved successfully: {stock_id_value}")
                except Exception:
                    stock_id_value = None
                    print(f"Stock code {code} failed to retrieve stockId")

                results.append({"code": code, "stockId": stock_id_value})

                time.sleep(random.uniform(2, 5))

            except Exception as e:
                print(f"Error processing {code}: {e}")
                continue

    finally:
        return results

# Request and parse data
def scrape_data(stock_ids):
    """Main scraping function"""
    all_data = []
    BASE_URL = "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en"

    with requests.Session() as session:
        for stock_id in stock_ids:
            try:
                time.sleep(random.uniform(1, 3))

                headers = {
                    "User-Agent": random.choice([
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0"
                    ]),
                    "Referer": BASE_URL,
                    "Origin": "https://www1.hkexnews.hk"
                }

                form_data = {
                    "lang": "EN",
                    "category": "0",
                    "market": "SEHK",
                    "searchType": "1",
                    "documentType": "-1",
                    "t1code": "40000",
                    "t2Gcode": "-2",
                    "t2code": "40100",
                    "stockId": stock_id.strip(),
                    "from": "20150101", # Change start date here (format: YYYYMMDD)
                    "to": "20250319", # Change end date here (format: YYYYMMDD)
                    "MB-Daterange": "0"
                }

                response = session.post(BASE_URL, data=form_data, headers=headers)
                soup = BeautifulSoup(response.text, "html.parser")

                table = soup.find("table", {"class": "table sticky-header-table table-scroll table-mobile-list"})
                if not table:
                    print(f"Stock code {stock_id} not found, possibly no data")
                    continue

                for row in table.find("tbody").find_all("tr"):
                    cols = row.find_all("td")
                    release_time = cols[0].find("span", class_="mobile-list-heading").next_sibling.strip()
                    stock_code = cols[1].find("span", class_="mobile-list-heading").next_sibling.strip()
                    stock_name = cols[2].find("span", class_="mobile-list-heading").next_sibling.strip()

                    doc_div = cols[3]
                    headline = doc_div.find("div", class_="headline").get_text(" ", strip=True)
                    link_tag = doc_div.find("a")
                    pdf_name = link_tag.text.strip()
                    pdf_url = "https://www1.hkexnews.hk" + link_tag["href"]
                    file_size = doc_div.find("span", class_="attachment_filesize").text

                    document = f"{headline}\n{pdf_name} ({file_size}) - {pdf_url}"

                    all_data.append([release_time, stock_code, stock_name, document, pdf_url])

                print(f"Successfully retrieved data for stock code {stock_code}")

            except Exception as e:
                print(f"Stock code {stock_id} processing failed - {str(e)}")
                continue

    return all_data

# Save data to Excel
def save_to_excel(data):
    """Save data to Excel"""
    df = pd.DataFrame(data, columns=["Release Time", "Stock Code", "Stock Short Name", "Document", "URL"])

    # Clean "Document" column, remove URLs
    df["Document"] = df["Document"].apply(lambda x: re.sub(r" - https?://[^\s]+", "", x))

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)

        worksheet = writer.sheets['Sheet1']
        for column in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in column)
            worksheet.column_dimensions[column[0].column_letter].width = max_length + 2

if __name__ == "__main__":
    stock_codes = load_stock_codes_from_txt()
    stock_id_results = get_stock_ids(stock_codes)
    stock_ids = [result["stockId"] for result in stock_id_results if result["stockId"] is not None]
    all_data = scrape_data(stock_ids)
    save_to_excel(all_data)
    print(f"Data has been saved to {OUTPUT_FILE}")
