from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os

#Set FPI base url with advisor ID
baseUrl = os.getenv('FN_BASE_URL')
idIn    = 102467
url     = baseUrl + str(idIn)

#Setup Selinium (using Chromium)
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

#Load page
print('get url...')
driver.get(url)

# Configure driver wait object
wait = WebDriverWait(driver, 5)

try:
    injectedLi = wait.until(presence_of_element_located((By.CLASS_NAME , "AlertItem")))
    print('found')
    data = injectedLi.text
    print(data)
finally:
    driver.close()
