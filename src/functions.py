import requests
import urllib
from bs4 import BeautifulSoup
import os
import io
import pandas as pd
import re
from itertools import chain
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import beatbox
import logging #log errors to file
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import xml.etree.ElementTree as ET
from lxml import html
from apiclient import discovery
from googleapiclient.discovery import build
import json
from add_titlescore import *


today = dt.today().date()

def assign_SDR(state):
    '''
    Function to map state to SDR
    :param state: 2 character str
    :return: str, SDR
    '''
    sdr_mapping ={'Matt Tager': ['IL', 'IN', 'IA', 'MI', 'MN', 'MO', 'ND', 'OH', 'SD', 'WI', 'WV', 'CA'],
    'Trevor Gori': ['AK', 'CT', 'HI', 'ID', 'NV', 'NH', 'NJ', 'RI', 'MA', 'ME', 'VT', 'DE', 'DC', 'MD', 'NY', 'PA', 'OR', 'WA', 'DE'],
    'Jonelle Miller' :['AL', 'AR', 'FL', 'GA', 'KY', 'LA', 'MS', 'NC', 'SC', 'TN', 'VA']}
    state = state.upper()
    for k, v in sdr_mapping.iteritems():
        if state in (v):
            sdr =k
        else:
            pass
    return sdr

def get_object_fields(svc, sf_object):
    '''
    function to get SalesForce object fields
    :param svc: beatbox connection
    :param sf_object: object to get description
    :return: list of column names

    ex: obj_desc = svc.describeSObjects("Conversion_Rate__c")
    '''

    obj_desc = svc.describeSObjects(sf_object)[0]
    names = [name for name in obj_desc.fields]
    return names

def login_beatbox_sandbox():
    # SANDBOX
    sf_user = 'gabrielle.agrocostea@collibra.com.salesopps'
    sf_token = 'lxKxphtvkKUQzuK6fmXnhUTL'
    sf_pass = 'C0llibr@'
    sf_pass_token = sf_pass + sf_token

    # instantiate object & login
    sf = beatbox._tPartnerNS
    svc = beatbox.PythonClient()
    # login to Sandbox
    svc.serverUrl = 'https://test.salesforce.com/services/Soap/u/20.0'
    svc.login(sf_user, sf_pass_token)
    return svc

def get_url_content(url):
    # get response and content into DataFrame
    resp = requests.get(url)
    content = resp.content
    content = content.decode('utf-8')
    # content = content.decode('utf-8', errors = 'ignore')
    return  content

def clean_extractor_data(dat):
    '''
    Function to clean up DataFrame from import.io Extractor (with indeed.com job data)
    Parse jobId and calculate posted date
    Remove jobs older than 30days
    :param dat: DataFrame
    :return: DataFrame
    '''

    # clean up data
    dat.columns = ['url', 'job_link', 'title', 'company_link', 'account_name', 'job_location', 'job_description', 'job_date' ]

    # regex patterns to find job ids - to deduplicate
    id_pattern = 'jk=(.*)\&'
    id_pattern_2 = '-([a-z0-9]+)'  # in case it doesnt follow the usual pattern starting with jk=

    # loc_pattern =  '([\w\s]+),\s(\w+)'
    loc_pattern = '([A-Za-z]+)'
    dat['jobId'] = dat.job_link.map(lambda x: re.findall(id_pattern, x))
    idx_empty = dat[dat.jobId.map(lambda x: x == [])].index
    dat.loc[dat.index.isin(idx_empty), 'jobId'] = dat[dat.index.isin(idx_empty)].job_link.map(
        lambda x: re.findall(id_pattern_2, x))
    dat.loc[dat.jobId.map(lambda x: len(x) > 1), 'jobId'] = dat[dat.jobId.map(lambda x: len(x) > 1)].jobId.map(
        lambda x: x[1])

    id_list = [i.pop() if type(i) == list else i for i in dat.jobId]
    dat['jobId'] = id_list
    # remove rows where there are duplicated jobids
    dat = dat[~dat.jobId.duplicated()]

    # determine date when job was posted
    # if it was a few hours ago then just put today's date as post_date
    dat['post_date'] = today
    dat['days_ago'] = 0
    dat.loc[dat.job_date.str.contains('days ago'),'days_ago'] = dat[
        dat.job_date.str.contains('days ago')].job_date.map(lambda x: re.findall('[0-9]+', x)).map(lambda x: int(x[0]))
    # remove older postings
    dat = dat[dat.days_ago <30]
    dat.loc[dat.job_date.str.contains('days ago'), 'post_date'] = dat[
        dat.job_date.str.contains('days ago')].job_date.map(lambda x: re.findall('[0-9]+', x)).map(
        lambda x: today - relativedelta(days=int(x[0])))
    dat = dat.drop('days_ago', axis =1)
    # capitalize names
    dat.account_name = dat.account_name.str.title()
    # pick only state from location

    ## TODO: fix this a bit - doesnt always work properly
    # dat.job_location = dat.job_location.map(lambda x: re.sub('[0-9]', '', str(x))).map(lambda x: re.findall(loc_pattern, x)).map(lambda x: x[0:1])

    #dat.job_location = dat.job_location.map(lambda x: re.findall(loc_pattern, x)).map(lambda x: x[0:2]).map(lambda x: " ".join(i for i in x))
    dat = dat.drop(['url','job_date'], axis=1)
    dat.job_description = dat.job_description.map(lambda  x: re.sub(r'[^\w]', ' ', x).strip())

    dat.columns = ['JobLink','JobTitle','CompanyLink','AccountName','JobLocation','JobDescription','JobId','PostDate']
    return dat


def google_results_url(query):
    '''
    Function to return top result of google search
    :param query: string (Company Name)
    :return:
    '''
    api_dir = '/Users/Gabi/Documents/dev/api_dir/'
    # api key
    api_key = open('/Users/Gabi/Documents/dev/api_dir/knowledge_graph.api_key').read()
    service = build("customsearch", "v1",
                    developerKey=api_key)
    result = service.cse().list(
            q=query,
            cx='014731159833071467066:uai-wgylptc'
        ).execute()

    return result["items"][0]["formattedUrl"]

def google_know_graph(query, api_key):
    '''
    Function to query google knowledge graph entities via google api
    :param query: string to query
    :param api_key: user google api_key
    :return: best scoring url and score
    '''
    query = query
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    params = {
        'query': query,
        'limit': 10,
        'indent': True,
        'key': api_key,
    }
    url = service_url + '?' + urllib.urlencode(params)
    response = json.loads(urllib.urlopen(url).read())
    for element in response['itemListElement']:
        return element['result']['url'] + ' (' + str(element['resultScore']) + ')'

def clean_companyLink(dat):
    '''
    Function to clean up company links if null
    :param dat: DataFrame
    :return: clean DataFrame
    '''
    dat.loc[dat.CompanyLink.isnull(),'CompanyLink'] = dat[dat.CompanyLink.isnull()].AccountName.map(lambda x: "https://www.google.com/#q=" + x)
    return dat

def is_google_url(compUrl):
    '''
    Function to check if company link is already google link
    :param compUrl: url from indeed. Ex: wwww.indeed.com/companyname
    :return: Boolean
    '''
    if re.findall('.*google.*', compUrl):
        return True
    else:
        return False


def sample_links(df):
    '''
    Function used to sample companyLink from DataFrame for testing purposes
    :param df: DataFrame with CompanyLink
    :return: str (company link from indeed.com)
    '''
    return df.CompanyLink.ix[randint(0, df.shape[0]-1)]


def add_title_score(dat):
    '''
    Function to add in titlescore to title column
    :param dat: DataFrame
    :return: DataFrame with titlescore for each title
    '''
    dat['cleanTitles'] = dat.JobTitle.map(lambda x: clean_title(title_string=x, mapping_dict=mapping))
    dat['temp'] = dat.cleanTitles.map(lambda x: get_title_points(title=x, level_list=levels, **title_mapping_args))
    dat['titleScore'] = dat.temp.map(lambda x: str(x[2]))
    dat = dat.drop(['temp', 'cleanTitles'], axis=1)
    return  dat



def update_data(svc, list_of_records):
    '''
    Function to update a list of records via beatbox
    Truncate conversion rates to 4 decimals

    :param svc: beatbox connection
    :param list_of_records: list of records to update, where each record is a dictionary
    '''
    for rec in list_of_records:
        conv_id = rec['Id']
        conv_rate = rec['ConversionRate__c']
        trunc_rate = np.float(str(conv_rate)[:6])
        rec_update = {'ConversionRate__c': trunc_rate, 'type': 'Conversion_Rate__c', 'Id': conv_id}

        # update eacb record
        results_updated = svc.update(rec_update)

        if results_updated[0]['success'] == True:
            print "Success truncating conversion rate"
        else:
            err_stat = results_updated[0]['errors'][0]['statusCode']
            print err_stat
            pass


def bulk_upload_data(svc, list_of_records):
    """
    Function to update a list of records via beatbox in bulk - chunks of 150
    limit is 200 records

    :param svc: beatbox connection
    :param list_of_records: list of records to update, where each record is a dictionary
    """
    all_records = []
    for rec in list_of_records:
        jobID = rec['JobId']
        account_name = rec['AccountName']
        job_location = rec['JobLocation']
        post_date = rec['PostDate']
        job_title = rec['JobTitle']
        job_link = rec['JobLink']
        job_description = rec['JobDescription']
        company_link = rec['CompanyLink']
        title_score = rec['titleScore']
        state = rec['State']
        rec_create = {'JobId__c': jobID, 'type': 'JobPosting__c',
                      'Account_Name__c': account_name, 'Job_Location__c': job_location, 'Post_Date__c': post_date,
                      'Job_Title__c': job_title, 'Job_Link__c':job_link, 'Company_Link__c': company_link,
                      'Job_Description__c': job_description, 'Title_Score__c':title_score, 'State__c':state
}
        all_records.append(rec_create)

    # update all records at once in bulk in chunks of 150 (limit is 200 rows)
    chunk_list = [all_records[x:x + 100] for x in xrange(0, len(all_records), 100)]
    for e, chunk in enumerate(chunk_list):
        results = svc.create(chunk)
        if results[0]['success']:
            print "Success updating stage movement, chunk ", e, " of ", len(chunk_list)
        else:
            err_stat = results[0]['errors'][0]['statusCode']
            print err_stat
            pass





def fuzz_best_match(companyLink, companyName, matching_function):
    """
    :param companyLink: link of company names to compare against
    :param companyName: string clean up
    :param matching_function: choices are: 'extractOne','token_sort_ratio', 'token_set_ratio'
    :return: tuple (target_name,  best_score)
	"""
    if matching_function == 'extractOne':
        f = process.extractOne
        target_link = f(companyLink, companyName)[0]
        target_score = f(companyLink, companyName)[1]
    else:
        if matching_function == 'token_sort_ratio':
            f = fuzz.token_sort_ratio

        elif matching_function == 'token_set_ratio':
            f = fuzz.token_set_ratio
        target_score = np.max([f(i, companyName) for i in companyLink])
        target_ix = np.argmax([f(i, companyName) for i in companyLink])
        target_link = companyLink[target_ix]

    return target_link, target_score