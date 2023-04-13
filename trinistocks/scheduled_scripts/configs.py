#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

dbusername = os.getenv("DJANGO_DB_USER")
dbpassword = os.getenv("DJANGO_DB_PASSWORD")
dbaddress = os.getenv("DJANGO_DB_HOST")
dbport = "3306"
schema = "trinistocksdb"
