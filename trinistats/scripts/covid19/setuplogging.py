#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""This module sets up logging using a /logs directory and tees the log output 
to the stdout stream and a filestream. The log files are then named <standard name><current datetime>.log
and are placed in the /logs directory. The logs directory is created in the working directory of the script that calls it.
Input parameters:
:param logfilestandardname: This sets the standard names for the logfiles. Eg.For logging for a module named parsestring.py, you can set the standard name to be 'parsestring'
:param logginglevel: Sets the logging level. Valid inputs are the levels listed in https://docs.python.org/3/library/logging.html#levels
:returns 0 if successful
:raises Exception if problems are encountered
"""
# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import logging
import sys
import os
from pathlib import Path
from datetime import datetime

# Put your function definitions here. These should be lower-case, separated by underscores.
def setuplogging(logfilestandardname,logginglevel,stdoutenabled):
        # Set the level of logging
        logging.basicConfig(level = eval(logginglevel))
        # Get the logging module
        logger = logging.getLogger()
        # Get the stdouthandler (the default handler in logger)
        stdouthandler = logger.handlers[0]
        # Set the format of the messages for logs
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # Get the parent folder of the current module
        parentfolder = str(os.path.dirname(os.path.realpath(__file__)))
        # Check the current working directory for a 'logs' folder to store our logfiles
        logdirectory = parentfolder+os.path.sep+'logs'
        # Create the directory if it doesn't already exist
        Path(logdirectory).mkdir(parents=True, exist_ok=True)
        # Now add a log handler to output to a file
        # And set the name for the logfile to be created
        logfilehandler = logging.FileHandler(logdirectory+os.path.sep+logfilestandardname+f"{datetime.now():%Y-%m-%d-%H-%M-%S}"+'.log')
        # Set the format for file log output messages
        logfilehandler.setFormatter(formatter)
        # Set the format for stdout log output messages
        stdouthandler.setFormatter(formatter)
        # Add the log handler for files
        logger.addHandler(logfilehandler)
        # Remove the log handler for stdout if we don't want it
        if stdoutenabled == 'False':
            logger.removeHandler(stdouthandler)
        return 0