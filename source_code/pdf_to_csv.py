#!/usr/local/bin/python3
"""
The purpose of the script is to convert the given pdf to csv.

Note:
1. This script is written to specifically parse the input pdf. While it may not work on other pdf files, it will certainly
   work on similar pdf (maybe another monthly current affairs journal from same vendor).

2.  I do believe we convert and gather the data in csv for training the AI/ML models. so made some design choices for better data.

3.  Some pages had multi-column layout - where text is split in columns. Though this could be parsed with additional effort. I have
    not included those pages in csv as they are better to be collected separately. Due to time constraint, I have not made code changes to
    include those pages.
"""
import pymupdf
import re
import os
import pandas as pd
import csv
from pymupdf import Document
import sys
import logging
from utils.get_logger import get_logger


def get_clean_pages(doc: Document) ->None:
    """
    This function goes thru all the pdf pages and deletes the unwanted ones.These deletions
    are decided after carefully examining the document.
    So these unwanted pages are the ones that has no valid data, such as
        1. Images and advertisement.
        2. Combination of images and text.
        3. Table of contents and Appendix pages
        4. Pages that has tricky embedded texts. Removing them after making sure they don't have valid article data.
    To target them particularly we have fetched the texts from such pages. For images/advertisements, we will not receive any text data.
    Note: Exceptions will be handled by our main function convert_it()

    :param doc: This is a document object of our pdf
    :rtype doc: pymupdf.Document
    :return: None
    """
    #Red flags are strings that we use to find the above-mentioned pages.
    red_flags = [
        'Ahmedabad |',
        'The Civil Services Examination is a rigorous test of knowledge,',
        'Copyright © by Vision IAS',
        'Table of Contents',
        'Quarterly Revision',
        'News Today is Daily Current',
        "Keep in mind, the Mains exam isn't just a stage within the UPSC CSE",
        "APPENDIX "
    ]
    #We loop thru pages and collect the page_numbers (0-indexed) that needs to be deleted
    pages_to_be_deleted = []
    for page in doc.pages():
        page_data = page.get_text()
        #if page_data has text data then page has text, we can go ahead and verify if it is a red flagged page or not. If page hsa no text,
        # delete it.
        if page_data:
            #we verify if the page has to be deleted by checking if the page-data has any of the strings from red_flag. If so delete them,
            for red_flag in red_flags:
                if page_data.count(red_flag):
                    pages_to_be_deleted.append(page.number)
                    break
        else:
            pages_to_be_deleted.append(page.number)
    # Once we have the page numbers that has to be deleted. we cannot delete them as is - since deleting from beginning changes the page order.
    # So, lets sort the page number list in descending order and delete from the back. This way, page order does not change.
    pages_to_be_deleted = sorted(pages_to_be_deleted, key=lambda x: -x)
    for page_no in pages_to_be_deleted:
        doc.delete_page(page_no)


def get_footer_start_pos(doc: pymupdf.Document) -> float:
    """
    This function finds the y0 co-ordinate where the footer starts. Since we have removed all the
    unwanted pages, footer (page number, website url) data will present at max y0 position(end of the page).
    This function gets the footer's y0 co-ordinate so that we can skip it during the conversion
    process.
    Decision that footer will always start at max y0 position is taken after carefully verifying
    each page programmatically. I could have hard-coded that value, just for reusability purpose
    I created this function.

    Note: Exception is handled by convert_it()
    :param doc: This is a document object of our pdf
    :rtype doc: pymupdf.Document
    :return: footer start position
    :rtype: float
    """
    # Using set here, to have unique values. It does not save much, but better than list.
    max_y0 = set()
    # Loop thru pages and get the text blocks in each page
    for page in doc.pages():
        blocks = page.get_text('blocks')
        if blocks:
            # now for some reason, footer appears randomly in the text blocks. So it is better to sort the text blocks by starting positions
            # y0 - descending and x0 - ascending. This way the starting position of the bottom most line (footer)
            # comes at first. we fetch the corresponding y0 value.
            val = sorted(blocks, key=lambda b: (-b[1], b[0]))[0][1]
            max_y0.add(val)
    # max of all y0 is considered. footer is guaranteed to be in position so we can skip anything that starts from y0.
    if not max_y0:
        # This should not happen but if there are pages in document and no blocks in any of it. then raise error.
        raise ValueError ('Empty pages - please check')

    return max(max_y0)

def get_data(doc: pymupdf.Document) ->dict[str,list]:
    """
    This function is the core of our script. It iterates through the pages of the PDF
    and extracts only the article-related data, such as the serial number, article title,
    and article body.
    To achieve this, we rely on the concept of bounding boxes (bbox) and their coordinates.
    Each page has coordinates similar to a map:
        (x0, y0) - the top-left corner of the page
        (x1, y1) - the bottom-right corner of the page
    As we move across the page, (x0, y0) and (x1, y1) change accordingly. Since we are
    interested in reading the text in natural order, we primarily use (x0, y0) for sorting,
    as it represents the reading flow. We do not need (x1, y1) for this task.

    Each page is divided into text blocks. Each block contains attributes such as
    x0, y0, x1, y1, block_number, and block_type.

    To process text in natural reading order, the blocks should be sorted by (y0, x0).

    We will go thru these blocks to get the article data.

    Note:    1. There are few pages with multi-column layout. They are not considered here.
             2.`page.get_text("blocks")` also returns blocks for images, which may not contain
               any text. As a result, there can be empty blocks that should be filtered out.

    Note: Exception will be taken care by convert_it()
    :param doc: This is a document object of our pdf
    :rtype doc: pymupdf.Document
    :return: article-data - this will be dictionary used to create pandas dataframe
    :rtype: dict[str,list]
    """

    # To identify Main topics and Article titles, we use regex patterns.
    main_topic_pattern = r'^\d{1,2}\.\s[A-Z]{3,}'
    article_pattern = r'^\d{1,2}\.\d{1,2}\.\s\w+'

    # Empty dictionary of list
    article_data = {'s_no':[],'article_title':[],'article_body':[]}

    # accumulate is a flag that we use to identify - if we are in the process accumulating article body of an article
    # or just encountered a new article for which we need to accumulate data(article body).
    # accumulate - False [We set it False during the start and every time we encountered new title]
    #              True [When we start accumulating the article body for an article]
    accumulate = False

    #article_body is the list that we use for accumulation.
    article_body= []

    #There are cases where two consecutive blocks have same data - so we compare prev_text block and current text block
    prev_text = ''

    # skip is a special flag to remove unwanted article.
    # article titled 'NEWS IN SHORTS' is common in 8 topics and has text block ordered differently - as text is in multi-column layout.
    # we are skipping this article as it does not work with our framework.
    skip= False

    #Getting the  footer starting position to safely to ignore it
    footer_start_pos = get_footer_start_pos(doc)
    # loop thru pages and process text blocks of each page
    for page in doc.pages():
        # Make sure we read the text blocks in the same natural reding order
        blocks = sorted(page.get_text('blocks'),key=lambda b: (b[1], b[0]))
        for block in blocks:
            x0,y0,x1,y1,text,_,_ = block
            # While accumulating text - Initially we concantated string as is. It didnt give best result. So, using below striping and replacing spaces.
            # This is not best but still better for training data.
            text= re.sub(r'\s+', ' ', text).strip()
            # skip empty blocks or the blocks
            if not text or not text.strip():
                continue
            #if this a footer, ignore this text block
            if y0 >= footer_start_pos - 0.5: #0.5 is for tolerance - sometimes checks on float dtype works strangely/
                continue
            #if this a main topic, ignore this text block
            elif re.match(main_topic_pattern,text):
                continue
            #if this is a article title, then go ahead
            elif re.match(article_pattern,text):
                #if accumulate is set to True - means we have been accumulating article title for previous article_title.
                # Append it, before we process next article title.
                if accumulate:
                    article_data['article_body'].append('\n'.join(article_body))
                #We reset skip every time we encounter new article title
                skip = False
                # Once we have article title hit, splitting the s_no and title using regex
                match_group = re.match(r'(^\d{1,2}\.\d{1,2}\.)\s(.*)',text)

                #if we have encountered  text from a multi-column layout page - we set skip as True and reset accumulate to False
                if match_group:
                    if match_group.group(2).count('NEWS IN SHORTS'):
                        skip = True
                        accumulate = False
                        article_body = []
                        continue
                    # if we have valid article - we store the s_no and article_title. We set accumulate to False and initialize article_body to start
                    # accumulation during next iteration.
                    article_data['s_no'].append(match_group.group(1))
                    article_data['article_title'].append(match_group.group(2))
                    accumulate = False
                    article_body = []
                else:
                    #if we are here, something is odd
                    raise RuntimeError ('Not able to retrieve title')
            else:
                # if accumulate is false - we are about start accumulation process.
                # we don't add text from multi-column layout pages and also avoid adding new lines in the beginning of the article body.
                if not accumulate and text != ' \n' and not skip:
                    article_body.append(text)
                    accumulate = True
                    prev_text = text
                #if accumulate is true, we accumulate the text as long it is not same as previous text
                if accumulate and text.strip() != prev_text.strip():
                    prev_text = text
                    article_body.append(text)
    #we are out of the loop - still last article body has to be retrieved, so lets get it.
    if accumulate and article_body:
        article_data['article_body'].append('\n'.join(article_body))
    return article_data


def to_csv(data: dict[str,list]) -> None:
    """
    This function uses pandas here to export the data to csv.
    Note : 1. 'utf-8-sig' is used for encoding  since my excel was not displaying special characters like
           bulletin points when 'utf-8' is used. If the exported csv is to be used for ingestion process then
           we can change the 'utf-8-sig' to 'utf-8' - to avoid formatting issues.
           2. Each csv fields are enclosed within quotes as requested.
    :param data: This is the parsed blocks of data for each articles. This is a dictionary with s_no,article_title,article_body
                 as keys that holds the corresponding array of data.
    :rtype data: dict[str,list]
    :return: None
    """
    try:
        df = pd.DataFrame(data)
        os.makedirs("../outputs", exist_ok=True)
        df.to_csv('../outputs/converted_to_pdf.csv',header=True,index=False,encoding='utf-8-sig',quoting=csv.QUOTE_ALL)
        logger.info(f"CSV written to {os.path.abspath('../outputs/converted_to_pdf.csv')}")
    except OSError:
        logger.error(f'Error occurred while exporting the csv file to {os.path.abspath("../outputs/converted_to_pdf.csv")}')
        raise
    except Exception as e:
        logger.error(f'Error occurred in to_csv() :{e}')
        raise






def convert_it():
    """
    This function uses pymupdf library for reading and parsing the pdf document. Parsed data is
    then converted to pandas dataframe which eventually exported as csv file.
    :return: None
    """
    try:
        logger.info('Starting the PDF conversion process')
        with pymupdf.open('../inputs/convert_me.pdf') as document:
            if document.page_count == 0:
                raise ValueError ('Empty document - Please check the inputs')
            get_clean_pages(document)
            a_data = get_data(document)
            to_csv(a_data)
        logger.info('PDF conversion has been successful')
    except pymupdf.FileNotFoundError:
        logger.error(f'File not found at {os.path.abspath("../inputs/convert_me.pdf")}')
        logger.exception('Full stack Trace:')
        sys.exit(3)
    except KeyboardInterrupt:
        logger.error('User has stopped the conversion process.')
        logger.exception('Full stack Trace:')
        sys.exit(2)
    except Exception as e:
        logger.error(f'An error occurred during the conversion process: {e}')
        logger.exception('Full stack Trace:')
        sys.exit(1)
    finally:
        logging.shutdown()




if __name__ == '__main__':
    # Let's initialize a logger object for this program
    logger = get_logger("pdf_to_csv")
    convert_it()

