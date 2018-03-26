import numpy as np
import pandas as pd
import requests
from lxml import etree
from bs4 import BeautifulSoup
import time
import datetime
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine, MetaData, String, Integer, Table, Column, ForeignKey
import quandl
from itertools import cycle
