import sys
import os
import json
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import numpy as np
import matplotlib.pyplot as plt
import glob
import re







# Add the parent directory to the path so we can import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper import scrape_game


def test_scrape_game():
    result = scrape_game(2020020001)

    assert len(result) == 3


