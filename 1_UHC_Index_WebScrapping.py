# Databricks notebook source

from bs4 import BeautifulSoup
import requests

url = 'https://transparency-in-coverage.uhc.com/'
page = requests.get(url)
soup = BeautifulSoup(page.text,'html')
print(soup)


# COMMAND ----------

# MAGIC %sh 
# MAGIC wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# MAGIC sudo dpkg -i google-chrome-stable_current_amd64.deb
# MAGIC sudo apt-get -f install -y

# COMMAND ----------

# MAGIC %pip install selenium
# MAGIC %pip install chromedriver-autoinstaller
# MAGIC %pip install requests

# COMMAND ----------

# Import necessary modules
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Automatically install the correct version of ChromeDriver
chromedriver_autoinstaller.install()

# Configure Chrome options for headless mode
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Initialize the WebDriver
driver = webdriver.Chrome(options=chrome_options)

# COMMAND ----------

from selenium.webdriver.common.by import By

url = "https://transparency-in-coverage.uhc.com/"
driver.get(url)

links = driver.find_elements(By.TAG_NAME, 'a')
urls = [link.get_attribute('href') for link in links]

for url in urls:
    print(url)

# COMMAND ----------

import re
html_source = driver.page_source
json_urls = re.findall(r'https?://[^ "]+\.json\?.*kk%3D"', html_source)
for json_url in json_urls:
    print(json_url)

# COMMAND ----------

# # Import necessary modules
# import chromedriver_autoinstaller
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options

# # Automatically install the correct version of ChromeDriver
# chromedriver_autoinstaller.install()

# # Configure Chrome options for headless mode
# chrome_options = Options()
# chrome_options.add_argument("--headless")
# chrome_options.add_argument("--no-sandbox")
# chrome_options.add_argument("--disable-dev-shm-usage")

# # Initialize the WebDriver
# driver = webdriver.Chrome(options=chrome_options)

# html = driver.page_source
# print(html)

# COMMAND ----------

from bs4 import BeautifulSoup

soup = BeautifulSoup(html_source, 'html.parser')

index_url_list = []
# Find all <div> tags with class "ant-space-item" containing an <a> tag
div_tags = soup.find_all('div', class_='ant-space-item')
for div_tag  in div_tags:
    a_tag = div_tag.find('a', href=True)
    if a_tag and '.json' in a_tag['href']:
        index_url_list.append(a_tag['href'])
print(index_url_list)

# COMMAND ----------

len(index_url_list)

# COMMAND ----------

thousandIndexList = index_url_list[:1000]

# COMMAND ----------

thousandIndexList

# COMMAND ----------

dbutils.fs.mkdirs('/NOV2024/')

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

import requests
from requests.exceptions import HTTPError

urls = thousandIndexList

# Function to download a file and save it to DBFS
def download_file(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful

        # Write the file content to DBFS
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded and saved to: {save_path}")
    
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # Handle HTTP errors
    except Exception as err:
        print(f"Other error occurred: {err}")  # Handle other errors

# Download each file and save to DBFS
for url in urls:
    file_name = url.split("/")[-1].split("?sp=")[0]
    local_directory = "/dbfs/UHN/"
    dbfs_path = os.path.join(local_directory, file_name)
    download_file(url, dbfs_path)


# COMMAND ----------

import requests
urls = thousandIndexList
def download_file_to_dbfs(url, dbfs_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  

        dbutils.fs.put(dbfs_path, response.content.decode('utf-8'), overwrite=True)
        print(f"File downloaded and saved to DBFS: {dbfs_path}")
    
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  
    except Exception as err:
        print(f"Other error occurred: {err}")

dbfs_directory = "/NOV2024/"

for url in urls:
    file_name = url.split("/")[-1].split("?sp=")[0]
    dbfs_path = f"{dbfs_directory}{file_name}"
    download_file_to_dbfs(url, dbfs_path)
    
display(dbutils.fs.ls(dbfs_directory))


# COMMAND ----------

display(dbutils.fs.ls('dbfs:/UHN2024/'))

# COMMAND ----------


