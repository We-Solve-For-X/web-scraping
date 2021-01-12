from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import csv
from scrape_profile import scrapeProfile
import re
import json
import threading
import urllib.request
import time
import queue
import logging
import os

log = logging.getLogger('scrape-profiles')
log.propagate = False

# setup analitics
startTime = time.time()
wErrors = 0
wSuccess = 0

# configure
#start 100 000
#end approx 161 000
idsRangeMin = 108920
idsRangeMax = 109000
idsTotal = idsRangeMax - idsRangeMin
maxThreads = 4
outFile = str(startTime) + "_ipf_profiles.csv"

# load queue
idsQueue = queue.Queue()
for b in range(idsRangeMin, idsRangeMax):
    idsQueue.put(b)

log.info("Started scraping")

#create file write semaphore
write_sema = threading.Semaphore(value=1)

# set scrape target base URL
baseUrl = os.getenv('FN_BASE_URL')
log.info("Scraping to base url: " + baseUrl)

def logCompletion():
    global wSuccess
    global wErrors
    completed = wSuccess + wErrors
    progress = (completed/idsTotal)*100
    progress = round(progress, 2)
    print("progress " + str(progress) + "%")
    log.info("progress " + str(progress) + "%")

# orchestartor function that is run on each thread
def threadProcess():
    global wSuccess
    global wErrors
    with open(outFile, mode='a') as profiles_files:
        try: 
            profiles_writer = csv.writer(profiles_files, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            while idsQueue.qsize() != 0:
                idIn = idsQueue.get()
                log.info("Starting profile: " + str(idIn))
                url  = baseUrl + str(idIn)
                driver.get(url)
                profile = scrapeProfile(driver, idIn, url)
                write_sema.acquire()
                profiles_writer.writerow(profile)
                write_sema.release()
                wSuccess += 1
                logCompletion()
                log.info("Finished profile: " + str(idIn))
        except:
            log.warning("Error while scraping profile")
            wErrors += 1
        driver.close()
        


#innitiate threads
threads = [threading.Thread(target=threadProcess) for i in range(0,maxThreads)]
for thread in threads:
    thread.start()

#await for threads to complete & rejoin
for thread in threads:
    thread.join()


# Calculate analytics
totalTime = time.time() - startTime

print ("Total pages scraped: " +  str(wSuccess))
print ("Total Errorsrrors: " +  str(wErrors))
print ("Processing Time: " +  str(totalTime))
print ("Time / writes (ms): " + str(totalTime * 1000 / (wSuccess)))