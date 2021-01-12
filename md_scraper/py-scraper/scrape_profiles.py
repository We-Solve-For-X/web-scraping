from bs4 import BeautifulSoup
import urllib.request
import re
import json
import threading
import urllib.request
import time
import queue
import logging

log = logging.getLogger('scrape-profiles')
log.propagate = False

log.warning("Requested scraping...")

# setup analitics
startTime = time.time()
wErrors = 0
wSuccess = 0

# configure
idsRangeMin = 0
idsRangeMax = 11000000
maxThreads = 200
unqSet = {0, 5120000, 10240000, 2005000, 7125000, 9685000, 170000, 4010000, 9130000, 6015000, 9855000, 4180000, 8020000, 1065000, 6185000, 10025000, 3070000, 8190000, 5075000, 10195000, 7080000, 9640000, 125000, 9085000, 2130000, 9810000, 4135000, 1020000, 6140000, 9980000, 3025000, 8145000, 5030000, 10150000, 3195000, 7035000, 9595000, 80000, 9040000, 2085000, 7205000, 9765000, 4090000, 9210000, 6095000, 9935000, 8100000, 10105000, 3150000, 9550000, 35000, 5155000, 10275000, 2040000, 7160000, 9720000, 4045000, 9165000, 6050000, 9890000, 8055000, 1100000, 6220000, 10060000, 3105000, 8225000, 9505000, 5110000, 10230000, 7115000, 9675000, 160000, 4000000, 9120000, 2165000, 6005000, 9845000, 4170000, 8010000, 1055000, 6175000, 10015000, 3060000, 8180000, 5065000, 10185000, 7070000, 9630000, 115000, 9075000, 2120000, 9800000, 4125000, 1010000, 6130000, 9970000, 3015000, 8135000, 9515000, 5020000, 10140000, 3185000, 7025000, 9585000, 70000, 9030000, 2075000, 7195000, 9755000, 4080000, 9200000, 6085000, 9925000, 8090000, 1135000, 10095000, 3140000, 9540000, 25000, 5145000, 10265000, 2030000, 7150000, 9710000, 195000, 4035000, 9155000, 6040000, 9880000, 8045000, 1090000, 6210000, 10050000, 3095000, 8215000, 5100000, 10220000, 7105000, 9665000, 150000, 9110000, 2155000, 9835000, 4160000, 8000000, 1045000, 6165000, 10005000, 3050000, 8170000, 5055000, 10175000, 7060000, 9620000, 105000, 9065000, 2110000, 9790000, 4115000, 9235000, 1000000, 6120000, 9960000, 3005000, 8125000, 5010000, 10130000, 3175000, 7015000, 9575000, 60000, 9020000, 10300000, 2065000, 7185000, 9745000, 4070000, 9190000, 6075000, 9915000, 8080000, 1125000, 10085000, 3130000, 9530000, 15000, 5135000, 10255000, 2020000, 7140000, 9700000, 185000, 4025000, 9145000, 6030000, 9870000, 4195000, 8035000, 1080000, 6200000, 10040000, 3085000, 8205000, 5090000, 10210000, 7095000, 9655000, 140000, 9100000, 2145000, 9825000, 4150000, 1035000, 6155000, 9995000, 3040000, 8160000, 5045000, 10165000, 3210000, 7050000, 9610000, 95000, 9055000, 2100000, 9780000, 4105000, 9225000, 6110000, 9950000, 8115000, 5000000, 10120000, 3165000, 7005000, 9565000, 50000, 5170000, 9010000, 10290000, 2055000, 7175000, 9735000, 4060000, 9180000, 6065000, 9905000, 8070000, 1115000, 6235000, 10075000, 3120000, 9520000, 5000, 5125000, 10245000, 2010000, 7130000, 9690000, 175000, 4015000, 9135000, 6020000, 9860000, 4185000, 8025000, 1070000, 6190000, 10030000, 3075000, 8195000, 5080000, 10200000, 7085000, 9645000, 130000, 9090000, 2135000, 9815000, 4140000, 1025000, 6145000, 9985000, 3030000, 8150000, 5035000, 10155000, 3200000, 7040000, 9600000, 85000, 9045000, 2090000, 7210000, 9770000, 4095000, 9215000, 6100000, 9940000, 8105000, 10110000, 3155000, 9555000, 40000, 5160000, 9000000, 10280000, 2045000, 7165000, 9725000, 4050000, 9170000, 6055000, 9895000, 8060000, 1105000, 6225000, 10065000, 3110000, 8230000, 9510000, 5115000, 10235000, 2000000, 7120000, 9680000, 165000, 4005000, 9125000, 2170000, 6010000, 9850000, 4175000, 8015000, 1060000, 6180000, 10020000, 3065000, 8185000, 5070000, 10190000, 7075000, 9635000, 120000, 9080000, 2125000, 9805000, 4130000, 1015000, 6135000, 9975000, 3020000, 8140000, 5025000, 10145000, 3190000, 7030000, 9590000, 75000, 9035000, 2080000, 7200000, 9760000, 4085000, 9205000, 6090000, 9930000, 8095000, 8235000, 1140000, 10100000, 3145000, 9545000, 30000, 5150000, 10270000, 2035000, 7155000, 9715000, 4040000, 9160000, 6045000, 9885000, 8050000, 1095000, 6215000, 10055000, 3100000, 8220000, 5105000, 10225000, 7110000, 9670000, 155000, 9115000, 2160000, 6000000, 9840000, 4165000, 8005000, 1050000, 6170000, 10010000, 3055000, 8175000, 5060000, 10180000, 7065000, 9625000, 110000, 9070000, 2115000, 9795000, 4120000, 9240000, 1005000, 6125000, 9965000, 3010000, 8130000, 5015000, 10135000, 3180000, 7020000, 9580000, 65000, 9025000, 2070000, 7190000, 9750000, 4075000, 9195000, 6080000, 9920000, 8085000, 1130000, 10090000, 3135000, 9535000, 20000, 5140000, 10260000, 2025000, 7145000, 9705000, 190000, 4030000, 9150000, 6035000, 9875000, 4200000, 8040000, 1085000, 6205000, 10045000, 3090000, 8210000, 5095000, 10215000, 7100000, 9660000, 145000, 9105000, 2150000, 9830000, 4155000, 1040000, 6160000, 10000000, 3045000, 8165000, 5050000, 10170000, 7055000, 9615000, 100000, 9060000, 2105000, 9785000, 4110000, 9230000, 6115000, 9955000, 3000000, 8120000, 5005000, 10125000, 3170000, 7010000, 9570000, 55000, 5175000, 9015000, 10295000, 2060000, 7180000, 9740000, 4065000, 9185000, 6070000, 9910000, 8075000, 1120000, 10080000, 3125000, 9525000, 10000, 5130000, 10250000, 2015000, 7135000, 9695000, 180000, 4020000, 9140000, 6025000, 9865000, 4190000, 8030000, 1075000, 6195000, 10035000, 3080000, 8200000, 5085000, 10205000, 7090000, 9650000, 135000, 9095000, 2140000, 9820000, 4145000, 1030000, 6150000, 9990000, 3035000, 8155000, 5040000, 10160000, 3205000, 7045000, 9605000, 90000, 9050000, 2095000, 7215000, 9775000, 4100000, 9220000, 6105000, 9945000, 8110000, 10115000, 3160000, 7000000, 9560000, 45000, 5165000, 9005000, 10285000, 2050000, 7170000, 9730000, 4055000, 9175000, 6060000, 9900000, 8065000, 1110000, 6230000, 10070000, 3115000}
steps = 5000
requestTimeout = 22
outFile = open("/home/ec2-user/web-scraping/profiles.txt", "a+")

# load queue
idsQueue = queue.Queue()
for u in unqSet:
    for b in range(0,steps):
        idsQueue.put(u + b)

log.warning("-> Started scraping " + str(idsQueue.qsize()) + " entries.")

#create file write semaphore
write_sema = threading.Semaphore(value=1)

# set scrape target base URL
#KEPT AS SECRET
baseUrl = ''

# file output writer
outFile.write('[')
def writeToFile(profile):
    out = profile + ", "
    outFile.write(out)

# extract qualifications from registration
def getQualifications( qualificationsList ):
    qsLength = len(qualificationsList)
    qualifications = []
    while qsLength > 0:
        qName = qualificationsList[qsLength - 2].text
        qDate = qualificationsList[qsLength - 1].text
        qualification = {
        "name": qName,
        "date": qDate
        }
        qualifications.append(qualification)
        qsLength -= 2
    return qualifications

def extractPractices( practiceList ):
    pracLength = len(practiceList)
    practices = []
    while pracLength > 0:
        pType = practiceList[pracLength - 6].text#.replace(u'\xa0', '').encode('utf-8')
        pField = practiceList[pracLength - 5].text#.replace(u'\xa0', '').encode('utf-8')
        pSpeciality = practiceList[pracLength - 4].text#.replace(u'\xa0', '').encode('utf-8')
        psSpeciality = practiceList[pracLength - 3].text#.replace(u'\xa0', '').encode('utf-8')
        pDate = practiceList[pracLength - 2].text#.replace(u'\xa0', '').encode('utf-8')
        pOrigin = practiceList[pracLength - 1].text#.replace(u'\xa0', '').encode('utf-8')

        pPractice = {
        "type": pType,
        "field": pField,
        "speciality": pSpeciality,
        "sub-speciality": psSpeciality,
        "date": pDate,
        "origin": pOrigin 
        }
        practices.append(pPractice)
        pracLength -= 6
    return practices

# process professionals & each registration
def extractRegistrations( registrationTables ):
    registrations = []
    for regTable in registrationTables:
        qualificatonsExt = []
        practisesExt = []
        
        if ((regTable.find('table', attrs={'id': re.compile('Qualifications')})) != None):
            qualificationsList = regTable.find('table', attrs={'id': re.compile('Qualifications')}).findAll('td')
            qualificatonsExt = getQualifications(qualificationsList)
        
        if ((regTable.find('table', attrs={'id': re.compile('gvCategories')})) != None):
            practiceList = regTable.find('table', attrs={'id': re.compile('gvCategories')}).findAll('td')
            practisesExt = extractPractices(practiceList)

        regNumber = regTable.find('span', attrs={'id': re.compile('REGNO')}).text
        regStatus = regTable.find('span', attrs={'id': re.compile('REG_STS')}).text
        regRegister = regTable.find('span', attrs={'id': re.compile('REG_NAME')}).text
        regBoard = regTable.find('span', attrs={'id': re.compile('BOARD_NAME')}).text
        registration = {
            "number": regNumber,
            "status": regStatus,
            "isActive": (regStatus == 'ACTIVE'),
            "register": regRegister,
            "board": regBoard,
            "qualifications": qualificatonsExt,
            "practice": practisesExt
        }
        registrations.append(registration)
    return registrations

# Process User Profile
def processProfile( soup, idIn ):

    registrationTables = soup.find('table', attrs={'id': 'ctl00_ContentPlaceHolder1_tblRegistration'}).findAll('tr', recursive=False)
    registrations = extractRegistrations(registrationTables)

    name = soup.find('span', attrs={'id': 'ctl00_ContentPlaceHolder1_lblFullname'}).text
    city = soup.find('span', attrs={'id': 'ctl00_ContentPlaceHolder1_lblCITY'}).text
    province = soup.find('span', attrs={'id': 'ctl00_ContentPlaceHolder1_lblPROVINCE'}).text
    postcode = soup.find('span', attrs={'id': 'ctl00_ContentPlaceHolder1_lblPOSTAL_CODE'}).text
    user = {
        "name": name, 
        "city": city, 
        "province": province, 
        "postcode": postcode,
        "registrations": registrations,
        "id": idIn
    }
    return user

def processPage( idIn ):
    url = baseUrl + str(idIn)
    global wSuccess
    global wErrors
    try:
        rawPage = urllib.request.urlopen(url, timeout=requestTimeout)
        page = BeautifulSoup(rawPage, 'html.parser')
        name = page.find('span', attrs={'id': re.compile('blFullname')}).text.replace(' ', '')
        if name: 
            profile = processProfile(page, idIn)
            jsProfile = json.dumps(profile)
            write_sema.acquire()
            writeToFile(jsProfile)
            write_sema.release()
            wSuccess += 1
            log.warning("T: " + str(wSuccess) + " - " + str(idIn))
    except:
        wErrors += 1
        log.warning("wERROR: " + str(idIn))

#main processing function - passed to each thread
def main():
  while idsQueue.qsize() != 0:
        idIn = idsQueue.get()
        processPage(idIn)

#innitiate threads
threads = [threading.Thread(target=main) for i in range(0,maxThreads)]
for thread in threads:
    thread.start()

#await for threads to complete & rejoin
for thread in threads:
    thread.join()

#close write file
outFile.write(']')
outFile.close() 

# take last timestamp & print stats
totalTime = time.time() - startTime

print ("Total pages scraped: " +  str(wSuccess))
print ("Total Errorsrrors: " +  str(wErrors))
print ("Processing Time: " +  str(totalTime))
print ("Time / request (ms): " + str(totalTime * 1000 / (len(unqSet))))
print ("Time / writes (ms): " + str(totalTime * 1000 / (wSuccess)))