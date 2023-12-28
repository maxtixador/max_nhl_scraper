import sys
import os
import json
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import numpy as np
import matplotlib.pyplot as plt

from utility.constants import *
from utility.functions import *
pd.set_option('display.max_columns', None)




if __name__ == "__main__":
    
    # date = datetime.now().strftime("%Y-%m-%d")
    game_id = 2023020361



    print(get_pbp(game_id=game_id))

    


    # print("Hello World!")