import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np
import yaml
import os

# get configuration file's information
config = yaml.load(open('config.yaml'), Loader=yaml.SafeLoader)

# connect to database
conn = sqlite3.connect(config["database_name"]+".db")
c = conn.cursor()