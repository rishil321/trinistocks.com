#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module sets up logging using a /logs directory and tees the log output
to the stdout stream and a filestream. The log files are then named <standard name><current datetime>.log
and are placed in the /logs directory. The logs directory is created in the working directory of the script that calls it.
Input parameters:
:param logfilestandardname: This sets the standard names for the logfiles. Eg.For logging for a module named parsestring.py, you can set the standard name to be 'parsestring'
:param logginglevel: Sets the logging level. Valid inputs are the levels listed in https://docs.python.org/3/library/logging.html#levels
:param stdoutenabled: Sets whether we want to log to the stdout stream as well
:returns 0 if successful
:raises Exception if problems are encountered
"""
# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import multiprocessing
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
import string
import logging.handlers
import smtplib

# CONSTANTS. These should be all in upper case

# Global variables
smtploggerindex = None
outlogginglevel = None

# Class definitions


class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    def __init__(self, mailhost, fromaddr, toaddrs, subject):
        # Set up the BufferingHandler with a capacity of 2048 log messages
        logging.handlers.BufferingHandler.__init__(self, 2048)
        self.mailhost = mailhost
        self.mailport = None
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)-5s %(message)s"))

    def flushoutput(self):
        # this method is automatically called
        # Send the log messages out to the email address specified
        if len(self.buffer) > 0:
            # Set the SMTP port
            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            # Connect to the SMTP mailhost
            smtp = smtplib.SMTP(self.mailhost, port)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (
                self.fromaddr, ",".join(self.toaddrs), self.subject)
            for record in self.buffer:
                s = self.format(record)
                msg = msg + s + "\r\n"
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
            self.buffer = []

# Put your function definitions here. These should be lower-case, separated by underscores.


def setup_logging(logfilestandardname: 'str: Words to prepend the log files for easy identification' = 'log',
                  logdirparent: 'str: The parent dir where you want the log dir created: eg: str(os.path.dirname(os.path.realpath(__file__)))' = str(os.getcwd()),
                  filelogginglevel: 'A valid Python logging level eg. logging.INFO, logging.ERROR etc.' = logging.DEBUG,
                  stdoutenabled: 'boolean: Whether to log to the console or not' = True,
                  stdoutlogginglevel: 'A valid Python logging level eg. logging.INFO, logging.ERROR etc.' = logging.DEBUG,
                  smtploggingenabled: 'boolean: Whether to log to an email address as well ' = False,
                  smtplogginglevel: 'A valid Python logging level eg. logging.INFO, logging.ERROR etc.' = logging.INFO,
                  smtpmailhost: 'str: The mailhost to use eg. localhost' = 'localhost',
                  smtpfromaddr: 'str: The address to send the email from' = 'Python default email',
                  smtptoaddr: 'list: A list of addresses to send the email to' = 'test@gmail.com',
                  smtpsubj: 'str: The subject of the email' = 'Test Python Email') -> 'int: 0 if successful':
    # Get the logging module
    q = multiprocessing.Queue()
    # this is the handler for all log records
    handler = logging.StreamHandler()
    # Set the format of the messages for logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # ql gets records from the queue and sends them to the handler
    ql = logging.handlers.QueueListener(q, handler)
    ql.start()
    mainlogger = logging.getLogger()
    mainlogger.setLevel(stdoutlogginglevel)
    # set the global variable as well
    global outlogginglevel
    outlogginglevel = stdoutlogginglevel
    # add the handler to the logger so records from this process are handled
    mainlogger.addHandler(handler)
    # Get the stdouthandler (the default handler in mainlogger)
    stdouthandler = mainlogger.handlers[0]
    # Check the current working directory for a 'logs' folder to store our logfiles
    logdirectory = logdirparent+os.path.sep+'logs'
    # Create the directory if it doesn't already exist
    Path(logdirectory).mkdir(parents=True, exist_ok=True)
    # Now add a log handler to output to a file
    # And set the name for the logfile to be created
    logfilename = logdirectory+os.path.sep+logfilestandardname + \
        f"{datetime.now():%Y-%m-%d-%H-%M-%S}"+'.log'
    logfilehandler = logging.FileHandler(logfilename)
    # Set the format for file log output messages
    logfilehandler.setFormatter(formatter)
    # Set the level of logging for files
    logfilehandler.setLevel(filelogginglevel)
    # Set the format for stdout log output messages
    stdouthandler.setFormatter(formatter)
    # Add the log handler for files
    mainlogger.addHandler(logfilehandler)
    # Remove the log handler for stdout if we don't want it
    mainlogger.removeHandler(stdouthandler)
    # Now set up the SMTP log handler
    if smtploggingenabled:
        smtploghandler = BufferingSMTPHandler(
            smtpmailhost, smtpfromaddr, smtptoaddr, smtpsubj)
        smtploghandler.setLevel(smtplogginglevel)
        mainlogger.addHandler(smtploghandler)
        # Get the index of the SMTP log handler
        for index, handler in enumerate(mainlogger.handlers):
            if isinstance(handler, BufferingSMTPHandler):
                # And store the index for the flush method
                smtploggerindex = index
    return ql, q


def flush_smtp_logger():
    # Find the SMTP log handler
    mainlogger = logging.getLogger()
    for handler in mainlogger.handlers:
        if handler.__class__ == BufferingSMTPHandler:
            # and flush the messages
            handler.flushoutput()


def logging_worker_init(q):
    # all records from worker processes go to qh and then into q
    qh = logging.handlers.QueueHandler(q)
    logger = logging.getLogger()
    logger.setLevel(outlogginglevel)
    logger.addHandler(qh)
