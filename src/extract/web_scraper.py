from config.logging import get_logger
from config.settings import get_settings
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from datetime import datetime

settings = get_settings()
logger = get_logger(module=__name__)
