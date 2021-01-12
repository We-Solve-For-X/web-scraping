from bs4 import BeautifulSoup
import urllib.request
import re
import json
import threading
import urllib.request
import time
import queue
import logging
from collections import defaultdict

log = logging.getLogger('extract_ids_logger')
log.propagate = False

# setup analitics
startTime = time.time()
wSuccess = 0
freqCount = defaultdict( int )

# configure
idsRangeMin = 0
idsRangeMax = 11000000
steps = 5000
sample = 200
maxThreads = 250
requestTimeout = 22

# load queue
idsQueue = queue.Queue()
for i in range(idsRangeMin,idsRangeMax,steps):
    for b in range(0,sample):
        idsQueue.put(i + b)

# create unique set
unqSet = set()

def round_down(num, divisor):
    return num - (num%divisor)

# set scrape target
#KEPT AS SECRET
baseUrl = ''

log.warning("Starting scrape...")

def processPage( idIn ):
    url = baseUrl + str(idIn)
    log.warning("at " + str(idIn))
    global wSuccess
    try:
        rawPage = urllib.request.urlopen(url, timeout=requestTimeout)
        page = BeautifulSoup(rawPage, 'html.parser')
        name = page.find('span', attrs={'id': re.compile('blFullname')}).text.replace(' ', '')
        if name:
            wSuccess += 1
            num = round_down(idIn, steps)
            unqSet.add(num)
            freqCount[num] += 1
            log.warning("w - " + str(num))
    except:
        log.warning("xx-F: " + str(idIn))


def main():
  while idsQueue.qsize() != 0:
        idIn = idsQueue.get()
        processPage(idIn)

threads = [threading.Thread(target=main) for i in range(0,maxThreads)]
for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

totalTime = time.time() - startTime

outFile = open("idRanges.txt", "a+")

outFile.write(str(unqSet))

outFile.close()

print ("Scraped: " +  str(wSuccess))
print ("Set: ")
print (str(unqSet))
print ("Freq Count: ")
print (freqCount)
print ("Processing Time: " +  str(totalTime))
print ("Time / request (ms): " + str(totalTime * 1000 / (idsRangeMax - idsRangeMin)))
print ("Time / write (ms): " + str(totalTime * 1000 / (wSuccess)))