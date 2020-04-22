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
 
# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def copy_images_facebook_mohtt():
    try:
        # open the browser
        logging.info("Now opening the Firefox browser")
        options = Options()
        options.headless = False
        options.accept_insecure_certs = True
        profile = FirefoxProfile()
        profile.set_preference('security.tls.version.enable-deprecated', True)
        driver = webdriver.Firefox(profile, options=options)
        moh_fb_url = "https://www.facebook.com/pg/MinistryofHealthTT/posts/?ref=page_internal"
        reports_present_on_page = True
        while reports_present_on_page:
            logging.info("Navigating to "+moh_fb_url)
            driver.get(moh_fb_url)
            # get all paragraph elements on the page
            p_elements = driver.find_elements_by_tag_name("p")
            # check the text in each p element
            report_links = []
            for p_element in p_elements:
                if "MoH COVID-19" in p_element.text:
                    # get the parent node of this element (should be a div)
                    div_parent = p_element.find_element_by_xpath("./..")
                    # the images are contained in the next div
                    following_div = div_parent.find_elements_by_xpath("following-sibling::div")
                    # then get all the a elements in the following div
                    a_elements = following_div[0].find_elements_by_tag_name("a")
                    # the image we want should be the first a_element
                    a_elements[0].click()
                    element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "spotlight"))
                    )
                    # download the image
                    spotlight_image = driver.find_element_by_class_name("spotlight").get_attribute("src")
                    urllib.request.urlretrieve(spotlight_image, "captcha.jpg")
                    # find the close button
                    new_frame_u = driver.find_element_by_tag_name("u")
                    u_parent = new_frame_u.find_element_by_xpath("./..")
                    i_parent = u_parent.find_element_by_xpath("./..").click()
                    pass
        return 0
    except:
        logging.info("Encountered an issue while trying to download the images.")
        raise
    finally:
            if 'driver' in locals() and driver is not None:
                # Always close the browser
                driver.quit()
                logging.info("Successfully closed web browser.")
                logging.info("Completed downloading of all COVID19 pdfs from PAHO website.")
 
def main():
    """Each function should have a docstring description as well"""
    try:
        # Set up logging for this module
        logsetup = setuplogging(logfilestandardname='pullpahostats', 
                                logginglevel='logging.INFO', stdoutenabled=True)
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        # Of course, you can also use inline comments like these wherever you want
        copy_images_facebook_mohtt()
        sys.exit(0) # Use 0 for normal exits, 1 for general errors and 2 for syntax errors (eg. bad input parameters)
    except Exception as exc:
        traceback.print_exc()
        logging.critical("Error encountered while running script "+os.path.basename(__file__))
        logging.critical(exc)
        sys.exit(1)
    
if __name__ == "__main__":
	main()