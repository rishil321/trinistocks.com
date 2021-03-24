#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A module used to set up queues for logging using multiple threads.
This module sets up logging using a /logs directory and tees the log output
to the stdout stream and a filestream. The log files are then named <standard name><current datetime>.log
and are placed in the /logs directory. The logs directory is created in the working directory of the script that calls it.
"""
# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import multiprocessing
import logging
import os
from pathlib import Path
from datetime import datetime
import logging.handlers
import smtplib
import sys

# CONSTANTS. These should be all in upper case

# Global variables

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
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s"))

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
                self.fromaddr,
                ",".join(self.toaddrs),
                self.subject,
            )
            for record in self.buffer:
                s = self.format(record)
                msg = msg + s + "\r\n"
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.quit()
            self.buffer = []


# Put your function definitions here. These should be lower-case, separated by underscores.


def setup_logging(
    loggername=__name__,
    logdirparent=str(os.getcwd()),
    filelogginglevel=logging.DEBUG,
    stdoutlogginglevel=logging.DEBUG,
    smtploggingenabled=False,
    smtplogginglevel=logging.INFO,
    smtpmailhost="localhost",
    smtpfromaddr="Python default email",
    smtptoaddr="test@gmail.com",
    smtpsubj="Test Python Email",
):
    """Setup the logging module to write logs to several different streams

    :param modulename: [The name of the module calling this function], defaults to 'log'
    :type modulename: str, optional
    :param logdirparent: [description], defaults to str(os.getcwd())
    :type logdirparent: [type], optional
    :param filelogginglevel: [description], defaults to logging.DEBUG
    :type filelogginglevel: [type], optional
    :param stdoutlogginglevel: [description], defaults to logging.DEBUG
    :type stdoutlogginglevel: [type], optional
    :param smtploggingenabled: [description], defaults to False
    :type smtploggingenabled: bool, optional
    :param smtplogginglevel: [description], defaults to logging.INFO
    :type smtplogginglevel: [type], optional
    :param smtpmailhost: [description], defaults to 'localhost'
    :type smtpmailhost: str, optional
    :param smtpfromaddr: [description], defaults to 'Python default email'
    :type smtpfromaddr: str, optional
    :param smtptoaddr: [description], defaults to 'test@gmail.com'
    :type smtptoaddr: str, optional
    :param smtpsubj: [description], defaults to 'Test Python Email'
    :type smtpsubj: str, optional
    :return: [description]
    :rtype: [type]
    """
    # Set up a queue to take in logging messages from multiple threads
    q = multiprocessing.Queue()
    # set up a stream handler for stdout
    stdout_handler = logging.StreamHandler()
    # Set the format of the messages for logs
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(stdoutlogginglevel)
    mainlogger = logging.getLogger(loggername)
    if mainlogger.hasHandlers():
        mainlogger.handlers.clear()
    mainlogger.setLevel(logging.DEBUG)
    # add the handler to the logger so records from this process are handled
    mainlogger.addHandler(stdout_handler)
    # Check the current working directory for a 'logs' folder to store our logfiles
    logdirectory = logdirparent + os.path.sep + "logs"
    # Create the directory if it doesn't already exist
    Path(logdirectory).mkdir(parents=True, exist_ok=True)
    # Now add a log handler to output to a file
    # And set the name for the logfile to be created
    logfilename = (
        logdirectory
        + os.path.sep
        + loggername
        + f"{datetime.now():%Y-%m-%d-%H-%M-%S}"
        + ".log"
    )
    logfilehandler = logging.FileHandler(logfilename)
    # Set the format for file log output messages
    logfilehandler.setFormatter(formatter)
    # Set the level of logging for files
    logfilehandler.setLevel(filelogginglevel)
    # Add the log handler for files
    mainlogger.addHandler(logfilehandler)
    # ql gets records from the queue and sends them to the handler
    ql = logging.handlers.QueueListener(q, stdout_handler, logfilehandler)
    ql.start()
    # Now set up the SMTP log handler
    if smtploggingenabled:
        smtploghandler = BufferingSMTPHandler(
            smtpmailhost, smtpfromaddr, smtptoaddr, smtpsubj
        )
        smtploghandler.setLevel(smtplogginglevel)
        mainlogger.addHandler(smtploghandler)
    mainlogger.info("Logging setup successfully!")
    return ql, q, mainlogger


def flush_smtp_logger():
    # Find the SMTP log handler
    mainlogger = logging.getLogger(LOGGERNAME)
    for handler in mainlogger.handlers:
        if handler.__class__ == BufferingSMTPHandler:
            # and flush the messages
            handler.flushoutput()


def logging_worker_init(q):
    # the worker processes write logs into the q, which are then handled by this queuehandler
    qh = logging.handlers.QueueHandler(q)
    logger = logging.getLogger(LOGGERNAME)
    logger.addHandler(qh)
    # remove the default stdout handler
    for handler in logger.handlers:
        if handler.__class__ == logging.StreamHandler:
            logger.removeHandler(handler)
    pass
