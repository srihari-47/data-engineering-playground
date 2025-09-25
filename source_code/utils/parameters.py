import os
BASE_URL = 'https://indianexpress.com/'
BUSINESS_URL = 'https://indianexpress.com/section/business/'
DB_PATH = '../outputs/db/isaras.db'
MAX_RETRIES = 3
REQUEST_TIMEOUT = 15
ROBOT_URL = 'https://indianexpress.com/robots.txt/'
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/100 Safari/537.36"
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ARTICLES
(ID INTEGER PRIMARY KEY AUTOINCREMENT,
TITLE TEXT UNIQUE NOT NULL,
AUTHOR TEXT,
PUBLICATION_DATE TEXT NOT NULL,
CONTENT TEXT NOT NULL)
"""

#Since we have made title as unique field - any duplicates will cause conflict. In such cases we wont do anything so duplicates are removed.
INSERT_QUERY = """
INSERT INTO ARTICLES(TITLE, AUTHOR, PUBLICATION_DATE, CONTENT) 
VALUES(?,?,?,?)
ON CONFLICT(TITLE) DO NOTHING"""

FETCH_QUERY ="""
    SELECT COUNT(*) as row_count
    FROM ARTICLES"""
