from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import csv


def getById(identifier, driver):
    try:
        result = driver.find_element(By.ID, identifier).text
    except:
        result = ""
    return result


def getWorkAddress(driver):
    try:
        # Configure driver wait object
        wait = WebDriverWait(driver, 5)
        injectedLi = wait.until(presence_of_element_located((By.CLASS_NAME, "AlertItem")))
        result = injectedLi.text
    except:
        result = ""
    return result


def scrapeProfile(driverPage, urlId, url):
    # Extract work address
    name = ""
    email = ""
    mobile = ""
    expertise = ""
    bio = ""
    workAddress = ""

    try:
        workAddress = getWorkAddress(driverPage)
        name = getById(
            'ctl01_TemplateBody_WebPartManager1_gwpciNewPanelEditorCommon2_ciNewPanelEditorCommon2_CsContact\.FullName',
            driverPage)
        email = getById(
            'ctl01_TemplateBody_WebPartManager1_gwpciNewPanelEditorCommon2_ciNewPanelEditorCommon2_CsContact\.Email',
            driverPage)
        mobile = getById(
            'ctl01_TemplateBody_WebPartManager1_gwpciNewPanelEditorCommon2_ciNewPanelEditorCommon2_CsContact\.MobilePhone',
            driverPage)
        expertise = getById(
            'ctl01_TemplateBody_WebPartManager1_gwpciNewQueryMenuCommon_ciNewQueryMenuCommon_ResultsGrid_Grid1_ctl00__0',
            driverPage)
        bio = getById(
            'ctl01_TemplateBody_WebPartManager1_gwpciNewPanelEditorCommon_ciNewPanelEditorCommon_FinPlannerArea\.MyBio',
            driverPage)
    except:
        print("Got profile data exception")

    csvRow = [urlId, name, email, mobile, expertise, workAddress, bio, url]
    return csvRow