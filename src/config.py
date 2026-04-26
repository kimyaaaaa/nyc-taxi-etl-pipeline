import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()

DB_URL = os.getenv("DB_URL")
BASE_URL = os.getenv("BASE_URL")
RAW_PATH = os.getenv("RAW_PATH")


def get_engine():
    """
    Creates and returns a SQLAlchemy engine.
    """
    if not DB_URL:
        raise ValueError("DB_URL not found in environment variables")

    return create_engine(DB_URL)
