#!/usr/local/bin/python3
"""
    This is a  scraper script that scraps the business news in the business section from https://indianexpress.com/. This works only for this site.
"""
import argparse
import json
import logging
import sys
import urllib.request
from time import sleep
from datetime import datetime
import requests
from typing import Optional, List
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from dateutil import parser as dateparser
from utils.parameters import TABLE_SCHEMA, INSERT_QUERY, FETCH_QUERY
from utils.get_logger import get_logger
from utils.parameters import (
    BUSINESS_URL,
    DB_PATH,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    ROBOT_URL,
    USER_AGENT,
)
from utils.DB_helper import DBHelper


def get_args() -> datetime:
    """
    This function simply parses the date argument from the command line.

    :return: date submitted via command line
    :rtype: datetime
    """
    try:
        # Just give a simple description and name for our script - this helps with usage message.
        parser = argparse.ArgumentParser(prog='web_scraper', description='Scrape web pages for given date')
        # Add the date argument to the parser.
        parser.add_argument('-d', '--date',required=True, help='Date to scrape - MM/DD/YYYY')
        # parse the input arguments from command line
        args = parser.parse_args()
        # return the input date as datetime object
        return dateparser.parse(args.date)
    except Exception as e:
        # in case if we have submitted an incorrect date input - we raise an exception.
        logger.error(f'Error occurred while parsing the date argument:{e}')
        raise


def check_robot_txt() -> bool:
    """
    Read robots.txt for the given URL and check if the /business/ section is allowed.

    Note:
    Some sites may not respond or may take long to timeout, so we use a short timeout.
    If robots.txt cannot be read, we proceed with scraping (return True).

    :return: True if scraping is allowed (or robots.txt not available), False otherwise
    :rtype: bool

    """
    try:
        with urllib.request.urlopen(ROBOT_URL, timeout=2) as response:
            content = response.read()
            # RobotFileParser.parse expects an iterable of lines (strings).
            text = content.decode(errors="ignore")
            lines = text.splitlines()
            robot_parser = RobotFileParser()
            robot_parser.parse(lines)
            return robot_parser.can_fetch(USER_AGENT, BUSINESS_URL)

    except Exception:
        # If robots.txt cannot be read, log and proceed.
        logger.info("Error occurred while reading robots.txt. Proceeding with scraping by default.")
        return True


def get_soup(url: str) -> BeautifulSoup | None:
    """
    Fetch the html data from the given link and return the Beautifulsoup Object.

    :return: BeautifulSoup object obtained from the html data of the url
    :rtype: BeautifulSoup

    """
    # We are using a for loop here for retries. Ideally we should not iterate more than
    # once. In case of failures, we retry as many times as give in MAX_RETRIES.
    retry_attempt = 0
    for attempt in range(MAX_RETRIES):
        try:
            # setting user-agent same as my browser
            headers = {"User-Agent": USER_AGENT}
            logger.info(f'Trying to hit {url}')
            response = requests.get(
                url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True, stream=False
            )
            r = response.text
            # In case of any issues while hitting the link - we raise exception
            response.raise_for_status()
            # closing the response after collecting the html data
            response.close()
            # return the beautiful soup object
            return BeautifulSoup(r, "html.parser")

        except requests.exceptions.RequestException as e:
            # We will re-try for max_times mentioned in parameter.py if exceeded then raise the exception.
            if retry_attempt <= MAX_RETRIES - 1:
                # Sleep using exponential backoff and retry
                backoff = 2 ** (retry_attempt + 1)
                logger.info(f"Request failed: {e}. Retry {retry_attempt + 1} in {backoff} seconds.")
                sleep(backoff)
                retry_attempt += 1
                continue
            else:
                logger.error("Max retries reached while attempting to fetch homepage.")
                # Let's propagate the exception as is - after exhausting retries.
                raise
        except Exception as e:
            # Do not retry for non-request related exceptions.
            logger.error(f"Unexpected error while trying to hit the URL: {e}")
            raise
    return None


def get_total_pages(start_page=1) -> int | None:
    """
    This function iterate thru pages in business section to get total number of pages.

    Instead of going through every single page, the function starts with the first page of the
    Business section and looks at the pagination controls to find the maximum page number shown
    (like "1 2 3 ... 10").

    Then, instead of assuming that’s the last page, it jumps directly to that max page number and
    checks the pagination again. If a new, higher max page number is found on that page, it jumps
    to that one next.

    This process repeats, jumping from one max page to the next, until no higher page number is
    found — meaning we've reached the true last page.

    That final page number is saved as total_pages.


    :param start_page: Starting page always be 1
    :type start_page: int
    :return: total number of pages in the business section
    :rtype: int
    """
    # all_pages stores the page numbers that we have encountered during the iterations.
    # From which we pick the max number.
    all_pages = [0, ]
    pg_no: Optional[int] = None

    # loop runs until we find the max page - we are not looping thru all pages but only
    # getting page_numbers from tag 'a' with class ='page-numbers'.
    # this usually gives us the min and max page_number to navigate. This way we can
    # hit the last page when we can't navigate further.
    logger.info('Getting the total_page count')
    while True:
        pages = []
        try:
            url = BUSINESS_URL + f'page/{start_page}/'
            logger.info(f'fetching pagination from {url}')
            headers = {"User-Agent": USER_AGENT}
            response = requests.get(url,headers = headers,timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(response.text,'html.parser')
            response.raise_for_status()
            response.close()
            tags = soup.find_all('a', attrs={'class': 'page-numbers'})
            # pages collects page numbers found in each iteration (from current page visited)
            for tag in tags:
                try:
                    # There is a possibility for page number to be a string such as 'next'.
                    # So we need to convert them to int before checking for max_pages.
                    pg_no = int(tag.text.replace(',', ''))
                    pages.append(pg_no)
                except ValueError:
                    # if we are here then it means we received a page number that is not an integer.
                    # Simply skip that and check for next page number
                    pass
            if pages:
                # During any iteration if the maximum of page_numbers found matches with already found maximum page_number
                # - we exit. This means there is no point in going further as we will keep getting same max page_number.
                if max(pages) == max(all_pages):
                    logger.info(f'total page count: {max(pages)}')
                    return max(pages)
                else:
                    # Append the page_numbers found during this iteration to all_pages
                    all_pages += pages
                    # We replace the start_page with the maximum of page_numbers found in this current iteration.
                    start_page = max(pages)
                    continue
            # something is wrong if we are here - so raise error.
            raise RuntimeError(f'No valid page numbers found in {BUSINESS_URL}')
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f'something is not right...') from e



def check_date(page_no: int) -> tuple | None:
    """
    This function check the tags (span with attribute opinion-date) found in the
    given page to find all the published dates of the articles. Then returns the
    minimum and maximum published dates of the given page.

    :param page_no: Page number on which we will checking the min and max published dates
    :type page_no: int
    :return: minimum published date and maximum published date
    :rtype: tuple
    """
    # we will checking for article date in the input page of the business section
    url = BUSINESS_URL + f'page/{page_no}/'
    soup = get_soup(url)
    tags = soup.find_all('span', attrs={'class': 'opinion-date'})
    dates = []
    for tag in tags:
        try:
            # we are using dateutil.parser here cause the dates found in the webpage is harder to parse normally.
            date = dateparser.parse(tag.text)
            dates.append(date)
        except Exception:
            # if there is an issue with parsing the tag.text as date, ignore and continue with next article.
            continue
    if dates:
        # Returning the min and max published dates of the articles found in the input page.
        return min(dates), max(dates)
    return None


def binary_page_search(start_page: int, end_page: int, target_dt: datetime) -> int:
    """
    This function implements a binary search to find the page where
    articles published on a specific target date are located.

    Design considerations:

    1. Articles are listed in descending order based on their published date.
    2. Articles published on the target date may span across multiple pages.

    Approach:

    - Since we don't know exactly where the articles of interest begin,
    we must search backward a few pages from the first page where the
    target date is found, to ensure we capture all relevant articles.

    - Once we've found a page containing the target date, we continue
    iterating forward until we reach articles published on the previous day.
    This ensures we collect **all** articles from the target date, even if
    they are spread across several pages.


    :param start_page: Starting page number of a window/range
    :type start_page: int
    :param end_page: Ending page number of a window/page
    :type end_page: int
    :param target_dt: target date is the date given by the user
    :type target_dt: datetime
    :return: start_location from which we need to iterate and fetch the articles.
    :rtype: int
    """
    # lo & hi are the endpoints (page numbers) of our window. Articles of interest
    # must reside in this window.
    lo, hi = start_page, end_page
    result = None

    # we shrink the window to find the exact page at which the articles of interest start
    # This continues until our lower endpoint crosses higher endpoint.
    while lo <= hi:
        # we find the midpoint of the window and use it to find in which half of the
        # window our articles are located.
        mid = (lo + hi) // 2

        # we get the min_date and max_date of every page at the midpoint
        dates = check_date(mid)
        if not dates:
            raise RuntimeError (f'We can get the min and max page numbers from page {mid}')
        min_date,max_date = dates
        if min_date <= target_dt <= max_date:
            # The target date’s articles might appear on this page or on earlier ones.
            # To be safe, record this page as a potential match and then also search toward earlier pages.
            # Only drawback - we will be checking earlier pages when we exactly know the location of the articles
            # of interest (min_date < target_dt < max_date)
            result = mid  # result is useful when the target_dt is exactly in between min_page and max_page
            hi = mid - 1
        elif target_dt < min_date:
            # target is older, so let's search from right
            lo = mid + 1
        else:
            # target is newer, so let's search from left
            hi = mid - 1
    return result if result is not None else lo



def get_article_links(pg_no: int, target_dt: datetime, total_pages: int) -> set | None:
    """
    This function helps us collect all the links of the article of interest (published on target_dt).
    Once we know the page number where our articles are located, we just need to iterate thru all the
    articles in that page and fetch the articles that were published on the date same as target date.

    We might also need to go to next page if the last article in that page was published on target_date.
    This could mean we might have more articles on netx page.

    :param pg_no: page number where our articles of interest are located
    :type pg_no: int
    :param target_dt: input date given by user
    :type target_dt: input date
    :param total_pages: total number of pages in the business section
    :type total_pages: int
    :return: Links of all the articles of interest
    :rtype: set
    """
    article_links = set()
    found_target = False
    while True:
        url = BUSINESS_URL + f'page/{pg_no}/'
        soup = get_soup(url)
        # we are fetching all the articles by looking for 'div' tag with class attribute as 'o-opin-article'
        articles = soup.find_all('div', attrs={'class': "o-opin-article"})
        if not articles:
            # if we cant find any articles in the given page. something is off. raise exception.
            raise RuntimeError(f'No valid articles found in page{pg_no}')
        for article in articles:
            try:
                # we are fetching the article publication date from 'span' tag and class attribute 'opinion-date'
                date_tag = article.find('span', attrs={'class': 'opinion-date'})
                pub_date = dateparser.parse(date_tag.text)

                if pub_date > target_dt:
                    # we haven't reached the articles of interest yet. so continue to find..
                    continue
                elif pub_date == target_dt:
                    # we have found the article of interest - let's get the link
                    found_target = True
                    # href can't fetched directly as there were multiple 'a' tags under 'div' tag (class =
                    # 'o-opin-article'). we are fetching the href that has article link by finding the 'a' tag with
                    # class attribute as 'opinion-news-title'.
                    link_tag = article.find('a', attrs={'class': 'opinion-news-title'})
                    if link_tag.get('href', None):
                        article_links.add(link_tag['href'])
                    continue
                else:
                    if article_links:
                        # if we are here then it means we have reached article that was published earlier than
                        # our target date so lets return with the collected article links.
                        return article_links
                    else:
                        # if nothing found, then raise exception
                        raise RuntimeError(f'No valid article links found for the given date{target_dt}')
            except Exception:
                raise RuntimeError('Error while parsing the article metadata')

        # If we are here means - we have collected all the articles that were published dn the target date.
        # We must go to next page to check if there are more articles published on the target date.
        if pg_no >= total_pages:
            # we cant go any further if we have hit the last page
            if article_links:
                return article_links
            # if nothing found even after reaching last page - raise exception
            raise RuntimeError(f'No valid article links found for the given date{target_dt}')
        # check if we have to go to next page to get more article links
        if found_target:
            pg_no += 1
        else:
            # we’ve gone past target date. We might have come to next page thinking there
            # will be more articles but nothing was found.
            if article_links:
                return article_links
            raise RuntimeError(f'No valid article links found for the given date{target_dt}')


def parse_article_link(link: str) -> tuple | None:
    """
    This function parses the article data from the article link that is sent as argument.
    Note: 1. There can be premium articles - which we are skipping.
    2. There are articles that are written by agencies -for them author will be None (NULL)
    3. There can be more than 1 author for an article. Each author names are separated by ';'

    :param link: article links of an article of interest
    :type link: str
    :return: article data (title, author_name, publication date, article body)
    :rtype: tuple
    """
    soup = get_soup(link)
    # Article content is located within script tag (type attribute as 'application/ld+json)
    json_scripts = soup.find_all('script', attrs={'type':'application/ld+json'})
    for script in json_scripts:
        # we will have some special escape sequences - json.loads() removes them and gives us the clean data.
        data = json.loads(script.string)
        # fetch the newsArticle and see if it is free
        if data.get('@type') == 'NewsArticle' and data.get('isAccessibleForFree') == 'True':
            row_data = tuple()
            # Let's get the article title.
            # article title is the only way we can find duplicates.
            # Though this can't be fully useful in removing duplicates. Sometimes same old articles will be republished
            # on different dates with same title which could be caught.
            row_data += (data['headline'],)

            # Let's fetch the author name - there might be one or more authors. Also, the article might be
            # from an agency in which case we update the author name as None (NULL)
            try:
                # Author data as far as i have seen comes as an iterable - List of dictionaries.
                author_data = iter(data['author'])
                author_name: Optional[str] = None
                for author in author_data:
                    if author['@type'] == 'Person':
                        if author_name is None:
                            author_name = author['name']
                        else:
                            author_name += f'; {author["name"]}'
                row_data += (author_name,)
            except TypeError:
                # Something is wrong in processing the author name. So let's update it as None(NULL).
                row_data += (None,)

            # Lets get the published timestamp - with timezone details
            # we could have simply used target_date given in the input as we only fetched article
            # published on that date.However, if we want this field to be used for something else -
            # then it is better store it as the timestamp.

            # SQLite does not have date/timestamp data types - we can store them as TEXT/INT/REAL.
            # We will pass the datetime object as is - so will be stored as TEXT. Still it will be
            # treated like date.
            pb_date = dateparser.isoparse(data['datePublished'])
            row_data += (pb_date,)

            # Lets get the article body.
            row_data += (data['articleBody'].replace("\xa0", ""),)
            if row_data:
                return row_data
            else:
                raise ValueError(f'An article does not have any data: {link}')
    return None



def update_db(row_data: List[tuple]) -> None:
    """
    This function simply creates the table (if not exists) and insert all
    the article data that we have fetched.

    Note: Exception will be handled by DB helper class and propagated.
    :param row_data: List of tuples
    :type  row_data: List[tuple]
    :return: None
    """
    with DBHelper(DB_PATH) as DB:
        DB.ensure_schema(TABLE_SCHEMA)
        count = DB.fetch(FETCH_QUERY, fetch_all=False)
        logger.info(f'Row count of Articles table before inserting: {count[0]}')
        DB.execute(INSERT_QUERY, row_data)
        count = DB.fetch(FETCH_QUERY, fetch_all=False)
        logger.info(f'Row count of Articles table after inserting: {count[0]}')


def main() -> None:
    """
    This function orchestrates entire process from scraping to updating db.
    :return: None
    """
    try:
        if check_robot_txt():
            # Getting the target_date given in the input for processing.
            target_dt = get_args()

            # Getting the total_pages available in the business section
            total_pages = get_total_pages()

            # Now that we have total number of pages available, we need to find the page where the articles published
            # on our target date resides. For this we use Binary search to get the target page number.
            target_page = binary_page_search(1, total_pages, target_dt)

            # now we have target_page from which we have to collect our articles. the article published on the target_date
            # can also be spanning multiple pages. So we have to iterate thru subsequent pages when needed.
            article_links = get_article_links(target_page, target_dt, total_pages)
            rows = []
            for link in article_links:
                row_content = parse_article_link(link)
                # Articles that are premium will return None. we need to ignore them
                if row_content:
                    rows.append(row_content)
            if not rows:
                raise RuntimeError(f'All articles published on that day were premium. so nothing we can do')
            logger.info(f'Total of {len(rows)} articles has to be inserted in the articles table')
            # Now it is time to insert the data on the table
            update_db(rows)
            logger.info('We successfully added the articles to our edtech db')
        else:
            raise RuntimeError('Scraper is trying to access disallowed data from website. Quitting gracefully')
    except KeyboardInterrupt:
        logger.error('User has stopped the conversion process.')
        logger.exception('Full stack Trace:')
        sys.exit(2)
    except Exception as e:
        logger.error(f'An error occurred during the  process: {e}')
        logger.exception('Full stack Trace:')
        sys.exit(1)
    finally:
        logging.shutdown()



if __name__ == "__main__":
    logger = get_logger("web_scraper")
    main()
