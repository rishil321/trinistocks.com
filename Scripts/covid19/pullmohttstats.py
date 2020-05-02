#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:param -s OR --second_parameter: The description of your second input parameter 
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""
 
# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import sys
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains 
from setuplogging import setuplogging
import logging
import urllib.request
import traceback
import os
import time
from pathlib import Path
from os import listdir
from os.path import isfile, join
try:
    from PIL import Image
except ImportError:
    import Image
import pytesseract
import cv2
import numpy as np
 
# Put your constants here. These should be named in CAPS.
SCROLL_PAUSE_TIME = 2
MOHTT_REPORTS_DIR_NAME = "mohtt_img_reports"
IMAGE_SIZE = 1800
BINARY_THREHOLD = 180

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def copy_images_facebook_mohtt(image_dir_path):
    try:
        # open the browser
        logging.info("Now opening the Firefox browser")
        options = Options()
        options.headless = True
        options.accept_insecure_certs = True
        profile = FirefoxProfile()
        profile.set_preference('security.tls.version.enable-deprecated', True)
        driver = webdriver.Firefox(profile, options=options)
        moh_fb_url = "https://www.facebook.com/pg/MinistryofHealthTT/posts/?ref=page_internal"
        logging.info("Navigating to "+moh_fb_url)
        driver.get(moh_fb_url)
        # scroll down on the page until the first report is found
        logging.info("Now trying to scroll to find case #1.")
        report_1_found = False
        while not report_1_found:
            logging.info("First report not found. Scrolling some more.")
            # Scroll down to bottom of page
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # click not now when facebook asks to sign in
            not_now_button = driver.find_element_by_id("expanding_cta_close_button")
            if not_now_button.text == 'Not Now':
                not_now_button.click()
            # get all paragraph elements on the page
            p_elements = driver.find_elements_by_tag_name("p")
            # check the text in each p element
            for p_element in p_elements:
                if p_element.text == "#MediaRelease: COVID-19 Update #1":
                    driver.execute_script("arguments[0].scrollIntoView();", p_element)
                    report_1_found = True
                    logging.info("First report found. Stopped scrolling.")
        logging.info("Found the 1st report from the MoH.")
        logging.info("Now downloading all reports on the page.")
        # get all paragraph elements on the page
        p_elements = driver.find_elements_by_tag_name("p")
        # check the text in each p element for keywords
        keywords = ['update #','update no']
        for p_element in p_elements:
            if any(s in p_element.text.lower() for s in keywords):
                try:
                    logging.info("Now trying to download image from: "+p_element.text)
                    # set the filename
                    filename = p_element.text
                    # get the parent node of this element (should be a div)
                    div_parent = p_element.find_element_by_xpath("./..")
                    # the images are contained in the next div
                    following_div = div_parent.find_elements_by_xpath("following-sibling::div")
                    # then get all the a elements in the following div
                    a_elements = following_div[0].find_elements_by_tag_name("a")
                    # the image we want should be the first a_element
                    a_elements[0].click()
                    element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "spotlight")))
                    # find the image on the page
                    spotlight_image = driver.find_element_by_class_name("spotlight").get_attribute("src")
                    # set the download path on the local machine
                    img_download_path = os.path.join(image_dir_path,filename+".jpg")
                    # download the image to the local machine
                    urllib.request.urlretrieve(spotlight_image, img_download_path)
                    logging.info("Successfully downloaded image.")
                    # find the close button
                    u_elements = driver.find_elements_by_tag_name("u")
                    for element in u_elements:
                        if element.text == "Close":
                            # click the close button
                            u_parent = element.find_element_by_xpath("./..")
                            u_parent.click()
                            break
                except Exception as exc:
                    logging.error("Unable to download image.")
                # click not now when facebook asks to sign in
                not_now_button = driver.find_element_by_id("expanding_cta_close_button")
                if not_now_button.text == 'Not Now':
                    not_now_button.click()
    except:
        logging.info("Encountered an issue while trying to download the images.")
        raise
    else:
        logging.info("Completed downloading of all images from MoH Facebook page.")
        return 0
    finally:
            if 'driver' in locals() and driver is not None:
                # Always close the browser
                driver.quit()
                logging.info("Successfully closed web browser.")

def extract_report_data_from_images(image_dir_path):
    file_list = [f for f in listdir(image_dir_path) if isfile(join(image_dir_path, f))]
    for file in file_list:
        if file.endswith('.jpg'):
            # We'll use Pillow's Image class to open the image and pytesseract to detect the string in the image
            img = Image.open(os.path.join(image_dir_path,file_list[0]))
            img = cv2.imread(os.path.join(image_dir_path,file_list[0]), 0)
            filtered = cv2.adaptiveThreshold(img.astype(np.uint8), 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 41,3)
            kernel = np.ones((1, 1), np.uint8)
            opening = cv2.morphologyEx(filtered, cv2.MORPH_OPEN, kernel)
            closing = cv2.morphologyEx(opening, cv2.MORPH_CLOSE, kernel)
            or_image = cv2.bitwise_or(img, closing)
            file_text = pytesseract.image_to_string(or_image)
            print(file_text)
            pass
        
    
def main():
    """Each function should have a docstring description as well"""
    try:
        # Set up logging for this module
        logsetup = setuplogging(logfilestandardname='pullpahostats', 
                                logginglevel='logging.INFO', stdoutenabled=True)
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        # set up the directory to store the images
        currentdir = os.path.dirname(os.path.realpath(__file__))
        mohtt_img_reports_dir = os.path.join(currentdir,MOHTT_REPORTS_DIR_NAME)
        # create the folder if it does not exist already
        Path(mohtt_img_reports_dir).mkdir(parents=True, exist_ok=True)
        # browse the MoH facebook page and download the images of the reports
        # copy_images_facebook_mohtt(mohtt_img_reports_dir)
        # extract the text from the images
        report_data = extract_report_data_from_images(mohtt_img_reports_dir)
        sys.exit(0) # Use 0 for normal exits, 1 for general errors and 2 for syntax errors (eg. bad input parameters)
    except Exception as exc:
        traceback.print_exc()
        logging.critical("Error encountered while running script "+os.path.basename(__file__))
        logging.critical(exc)
        sys.exit(1)
    
if __name__ == "__main__":
	main()