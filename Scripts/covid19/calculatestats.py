#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
This script pulls raw data entered from the MOH and computes the data per day
:returns: 0
:raises Exception if any issues are encountered
"""
 
# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import sys
import trinistatsconfig
import logging
import traceback
from sqlalchemy import create_engine, Table, select, MetaData, bindparam
from sqlalchemy.dialects.mysql import insert
from setuplogging import setuplogging
 
# Put your constants here. These should be named in CAPS.
DBUSER = trinistatsconfig.dbusername
DBPASS = trinistatsconfig.dbpassword
DBADDRESS = trinistatsconfig.dbaddress
DBSCHEMA = trinistatsconfig.schema

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def calculatedailytests():
    """
    Use data from the ministry to calculate the daily data to graph later
    """
    dbengine = None
    dbengine = create_engine("mysql://"+DBUSER+":"+DBPASS+"@"+DBADDRESS+"/"+
                            DBSCHEMA, echo=False)
    # Try to create a connection to the db
    with dbengine.connect() as dbcon:
        logging.info("Successfully connected to database")
        # Reflect the tables already created in our db
        covid19casestable = Table('covid19cases', MetaData(), autoload=True, autoload_with=dbengine)
        covid19dailydatatable = Table('covid19dailydata', MetaData(), autoload=True, autoload_with=dbengine)
        # Get the columns that we require from the main cases table
        selectstmt = select([covid19casestable.c.date, covid19casestable.c.numtested, 
                             covid19casestable.c.numpositive, covid19casestable.c.numdeaths,
                             covid19casestable.c.numrecovered])
        result = dbcon.execute(selectstmt)
        logging.info("Fetched data from DB.")
        # create a list of dictionaries to store data fetched from the db
        covid19casesdata = []
        # also create a set to store unique dates
        covid19uniquedates = set()
        for row in result:
            covid19casesdata.append(dict(date=row[0],numtested=row[1],numpositive=row[2],
                                         numdeaths=row[3],numrecovered=row[4]))
            covid19uniquedates.add(row[0].date())
        logging.info("Added dates from DB.")
        # ensure that our set of dates is sorted
        covid19uniquedates = sorted(covid19uniquedates)
        # create a list of dictionaries to write back to the db. Set the first set of values to be 0
        covid19dailydata = [dict(date=covid19uniquedates[0],dailytests=0,
											dailypositive=0,dailydeaths=0,
											dailyrecovered=0)]
        # for each unique date, we need to get the total number of each type of case 
        for index in range(1,len(covid19uniquedates)):
			# to do this, we need to calculate the difference between the current day values and the last
                # find the highest number of the values for today (at the current index value in the sorted date list)
                maxtestedtoday = 0
                maxpositivetoday = 0
                maxdeathstoday = 0
                maxrecoveredtoday = 0
                for casedata in covid19casesdata:
                    # find the max number stored for this uniquedate for each field
                    if casedata['date'].date() == covid19uniquedates[index]:
                        if casedata['numtested'] > maxtestedtoday:
                            maxtestedtoday = casedata['numtested']
                        if casedata['numpositive'] > maxpositivetoday:
                            maxpositivetoday = casedata['numpositive']
                        if casedata['numdeaths'] > maxdeathstoday:
                            maxdeathstoday = casedata['numdeaths']
                        if casedata['numrecovered'] > maxrecoveredtoday:
                            maxrecoveredtoday = casedata['numrecovered']
                # now find the max number stored for each field for the day before
                maxtestedyesterday = 0
                maxpositiveyesterday = 0
                maxdeathsyesterday = 0
                maxrecoveredyesterday = 0
                for casedata in covid19casesdata:
                    if casedata['date'].date() == covid19uniquedates[index-1]:
                        if casedata['numtested'] > maxtestedyesterday:
                            maxtestedyesterday = casedata['numtested']
                        if casedata['numpositive'] > maxpositiveyesterday:
                            maxpositiveyesterday = casedata['numpositive']
                        if casedata['numdeaths'] > maxdeathsyesterday:
                            maxdeathsyesterday = casedata['numdeaths']
                        if casedata['numrecovered'] > maxrecoveredyesterday:
                            maxrecoveredyesterday = casedata['numrecovered']
                # Calculate the differences in values to get our daily values (values added today)
                dailytested = maxtestedtoday - maxtestedyesterday
                dailypositive = maxpositivetoday - maxpositiveyesterday
                dailydeaths = maxdeathstoday - maxdeathsyesterday
                dailyrecovered = maxrecoveredtoday - maxrecoveredyesterday
                # create and add the dictionary to the list
                covid19dailydata.append(dict(date=covid19uniquedates[index],dailytests=dailytested,
                                            dailypositive=dailypositive,dailydeaths=dailydeaths,
                                            dailyrecovered=dailyrecovered))
        # now write our list to the db
        insert_stmt = insert(covid19dailydatatable).values(covid19dailydata)
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
            date=insert_stmt.inserted.date,
            dailytests=insert_stmt.inserted.dailytests,
            dailypositive=insert_stmt.inserted.dailypositive,
            dailydeaths=insert_stmt.inserted.dailydeaths,
            dailyrecovered=insert_stmt.inserted.dailyrecovered,
        )
        result = dbcon.execute(on_duplicate_key_stmt)
        logging.info("Number of rows affected in the covid19dailydata table was "+str(result.rowcount))
        return 0
                    
 
def main():
    """
    Main function to calculate COVID19 stats
    """
    try:
        logsetup = setuplogging(logfilestandardname='calculatestats',logginglevel='logging.DEBUG', stdoutenabled=True)
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        calculatedailytests()
    except:
        logging.error("Problem was encountered in script.")
        logging.error(traceback.format_exc())
        sys.exit(1)
    else:
        logging.info("Script executed successfully.")
        sys.exit(0) # Use 0 for normal exits, 1 for general errors and 2 for syntax errors (eg. bad input parameters)
 
if __name__ == "__main__":
	main()