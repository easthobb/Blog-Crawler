from sqlalchemy import create_engine  
from sqlalchemy import Column, String, Integer, Date, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
import pandas as pd