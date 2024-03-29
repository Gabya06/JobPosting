import os
import pandas as pd
import numpy as np
import re
import beatbox


delims = ",", ".", "/", "&", "-", ' '
regexPattern = '|'.join(map(re.escape, delims))


# scoring_file: scoring matrix
#title_file  = 'All_Leads_for_Scoring_26Aug16.xlsx'
scoring_file = 'title_scoring.xlsx'
title_path = '/Users/Gabi/Documents/dev/job_postings'
data_dir = title_path + '/data/input_data/'


def load_xl(root_dir, file_name, sheetname, encoding='utf-8'):
    """
    :param root_dir: directory of data to load
    :param file_name: name of file
    :param sheetname: xl sheetname
    :param encoding: 'utf-8'
    :return: DataFrame
	"""
    xl = pd.ExcelFile(root_dir + file_name)
    sheet = sheetname
    df = xl.parse(sheet)
    return df

def login_beatbox():
    # login to SF via beatbox
    # SF user/pass
    sf_user = 'john.angerami@collibra.com'
    sf_token = 'TFAB49jVswWVquu75y9nAklhl'
    sf_pass = 'Fiske!418'
    sf_pass_token = sf_pass + sf_token

    # instantiate object & login
    sf = beatbox._tPartnerNS
    svc = beatbox.PythonClient()
    svc.login(sf_user, sf_pass_token)
    dg = svc.describeGlobal()
    return svc

def query_data_bb(svc, query_string):
    """
    Function using BeatBox to query data and return results for any query

    :param svc: beatbox connection
    :param query_string: query string
    :return: list with data
    """
    record_list = []

    query_res = svc.query(query_string)
    record_list += query_res['records']

    # if need to get more records
    while not query_res.done:
        print " ******** FETCHING MORE DATAS ********"
        query_res = svc.queryMore(query_res.queryLocator)
        record_list += query_res
    # df = pd.DataFrame(data=record_list)
    # df.columns = df.columns.map(lambda x: x.replace("__c", "").replace("_", ""))

    return record_list

def clean_column(column_name):
    """
    :param column_name: name of column to clean
    :return: clean string (column name)
	"""
    col_name = column_name.replace(' ', '').replace('[^\w\s]', '').replace('\\', '').replace('/', '')
    col_name = col_name.lower()
    return col_name


def clean_data(dat, mapping_dict):
    """
    :param dat: DataFrame - must have column called "title"
    :param mapping_dict: dictionary with old to new title values to map out. {'old':'new'}
    :return: DataFrame
	"""
    df = dat.copy()
    cols = df.columns
    cols = [clean_column(c) for c in cols]
    df.columns = cols

    df = df[~df.title.isnull()]
    df.title = df.title.str.lower()
    df.title = df.title.str.encode('utf-8')
    df = df[~df.title.isnull()]
    df = df[~df.title.isin(['none', 'mrs', 'mr', 'other', 'intern', 'unknown', 'no longer with'])]
    df = df[~df.title.str.contains('no longer with')]

    # remove non ascii
    pattern = re.compile('[\W_]+')
    df['title_split'] = df.title.map(lambda x: ''.join([y for y in pattern.sub(' ', x)]))
    # split up title by pattern (to list)
    df.title_split = df.title_split.str.split(regexPattern)
    # remove non alpha characters & drop empties
    df.title_split = df.title_split.map(lambda x: [i for i in x if i.isalpha() and len(i) > 1])
    # look up each title in list in mapping dict
    df.title_split = df.title_split.map(
        lambda x: [mapping_dict['new'].get(y) if mapping_dict['new'].get(y) is not None else y for y in x])
    df.title_split = df.title_split.map(lambda x: " ".join(i for i in x))
    df = df[df.title_split != '']

    return df


def clean_title(title_string, mapping_dict):
    # type: (str, dict) -> str
    '''
    :param title_string: title to be cleaned
    :param mapping_dict: dictionary with old to new title values to map out. {'old':'new'}
    :return: cleaned title string
    '''
    # remove non ascii
    pattern = re.compile('[\W_]+')

    title = title_string.lower().decode('utf-8')
    title = ''.join([y for y in pattern.sub(' ', title)])
    title = title.split(regexPattern)
    title  = [mapping_dict['new'].get(y) if mapping_dict['new'].get(y) is not None else y for y in title]
    title = " ".join(i for i in title)

    return  title


def get_nomatch_points(level, level_no_match):
    # type: (str, dict) -> int
    """
    :param level: level to get points for
    :param level_no_match: d['points']['vp'] = 13
    :return: points (int)
    """
    return level_no_match['points'][level]


def keyword_points(keyword, keyword_mapping):
    """ Returns points assigned to a keyword
    :param keyword: keyword string to look up
    :param keyword_mapping: {'keyword':points} dictionary
    :return: points (int)
	"""
    points = keyword_mapping['points'][keyword]
    return points


def get_points(level, clean_title, d):
    # type: (str, str, dict ) -> int
    """
    :param level: job_level string
    :param clean_title: title string
    :param d: dictionary w. levels & target_title. Ex: to look up 'vp data governance': d['vp']['data governance']
    :return: points (int) based on job_level and title (matrix lookup)
	"""
    points = d[level][clean_title]
    return points


def get_best_level(levels_to_compare, levels_ranked):
    """
    :param levels_ranked: list of levels ranked in order of best to worse
    :param levels_to_compare: list of levels which need to be compared to get best one
    :return: highest job level
    :rtype: str
	"""
    level_list = levels_to_compare
    levels_ordered_list = levels_ranked

    best_idx = np.argmin([levels_ordered_list.index(i) for i in level_list])
    best_level = level_list[best_idx]
    return best_level


def get_best_target(found_target, d, **best_level_args):
    """ Lookup the best possible score for a title w multiple joblevels and target_titles
	Returns highest possible score
    :type best_level_args: dict
    :param found_target: target_title string to look up in matrix
    :param d: dictionary with levels and target_title. Ex: d['vp']['data governance']
    :param best_level_args: {'levels_to_compare':job_levels, 'levels_ranked': levels_ranked}
	"""

    best_level = get_best_level(**best_level_args)
    points = []
    for target in found_target:
        points.append(get_points(best_level, target, d))
    best_idx = np.argmax([points])
    best_target = found_target[best_idx]
    return best_target


def map_levels(level_mapping_dict, level_list):
    # type: (dict, list) -> str
    """ Map job_level to levels in SFDC
    :param level_mapping_dict: {'Actual':'SFDC'} level mapping (ex:'administrative':'individual contributer')
    :param level_list: list of job_levels to map
    :return: correct SFDC job_level
	"""
    mapped_levels = [level_mapping_dict['SFDC'][k] for k in level_list]
    mapped_levels = list(set(mapped_levels))
    return mapped_levels


def get_levels(title, level_list):
    """ Parse title and return job levels
    :param title: title string to parse
    :param level_list: list of levels to try and match up in title string
    :return: job_levels and title
	"""
    job_levels = []
    for i in level_list:
        if title.find(i) != -1:
            job_levels.append(i)
            title = title.replace(i, '').strip()
    return job_levels, title


def has_target_title(title, target_title_list):
    ''' Return all target titles found in a title
	@params:
		title: title string
		target_title_list: list of target titles to try and match up in title string
	'''
    found_target = []
    for target in target_title_list:
        if target in title:
            found_target.append(target)
            title = title.replace(target, '')
    return found_target


def flatten_list(l):
    """ flatten a list of lists
    :param l: list to flatten
    :return: flattened list of lists
	"""
    flattened = []
    for sublist in l:
        if type(sublist) is not list:
            flattened.append(sublist)
        else:
            flattened.extend(val for val in sublist)
    return flattened


def has_multiple_keywords(title, keyword_list):
    # type: (str, list) -> list
    """ Return keywords within a title
    :rtype: list
    :param title: title string
    :param keyword_list: list of keywords to try and match up in title string
    :return:
	"""
    keys = []
    to_check = []
    # see if keywords are joined by 'and'
    if 'and' in title:
        words = title.split('and')
        words = [i.strip() for i in words]

        for word in words:
            if word in keyword_list:
                keys.append(word)
            else:
                try:
                    w = word.split(' ')
                    for i in w:
                        if i in keyword_list:
                            keys.append(i)
                except:
                    pass

    else:
        # 1st pass at keywords being in title
        for k in keyword_list:
            if k in title:
                keys.append(k)
                title = title.replace(k, '').strip()

    multiple_keys = flatten_list(keys)
    return multiple_keys


def get_title_points(target_title_list, level_mapping_dict, points_dict, keyword_mapping_dict, levels_ranked,
                     keyword_list, levels_not_match, title, level_list):
    # type: (list, dict, dict, dict, list, list, list, str, list) -> tuple
    """
    :param target_title_list: list of target titles to look for (target_titles)
    :param level_mapping_dict: dict mapping to SFDC levels
    :param points_dict: dict with [level][title]:points mapping
    :param keyword_mapping_dict: dict with [keyword]:points mapping
    :param levels_ranked: ordered list of levels (best to worse)
    :param levels_not_match: dict with [level]:points mapping for standalone levels
    :param title: input title string to get points for
    :param level_list: list of levels to look for in get_levels()
    :return: tuple: (job_level, new_title, score)
	:type keyword_list: list
	"""
    points = 0

    # split up title and get job_level, map it and get target_title is possible
    job_levels, remainder = get_levels(title, level_list)
    job_levels = map_levels(level_mapping_dict, level_list=job_levels)
    targets_found = has_target_title(title, target_title_list)

    # if perfect match to target-titles
    if title in target_title_list:
        points = points_dict['individual contributer'][title]
        title_level = 'individual contributer'
        new_title = title
        return title_level, new_title, points

    # if there s a target_title and job_level: find best job_level & look up score for that target_title & job_level in matrix
    elif targets_found and job_levels:
        best_level_args = {'levels_to_compare': job_levels, 'levels_ranked': levels_ranked}
        best_target = get_best_target(found_target=targets_found, d=points_dict, **best_level_args)

        title_level = get_best_level(**best_level_args)
        new_title = best_target
        points = get_points(level=title_level, d=points_dict, clean_title=best_target)
        return title_level, new_title, points

    # target & no title - assign individual contributer and look up score for that target_title in matrix
    elif targets_found:
        best_level_args = {'levels_to_compare': ['individual contributer'], 'levels_ranked': levels_ranked}
        best_target = get_best_target(found_target=targets_found, d=points_dict, **best_level_args)

        title_level = 'individual contributer'
        new_title = best_target
        points = get_points(level=title_level, d=points_dict, clean_title=best_target)
        return title_level, new_title, points

    # if no target_title found, loop through keywords and lookup score for keywords
    # new title composed of keywords
    else:
        points = 50
        keys = has_multiple_keywords(title=remainder, keyword_list=keyword_list)

        # points for keywords
        if keys:
            new_title = ' '.join(k for k in keys)
            for k in keys:
                try:
                    points += keyword_points(k, keyword_mapping_dict)
                except:
                    print "Error", keys

        else:
            # otherwise no keywords in title
            new_title = np.nan

        # if any levels - get top level and assign that score, ignore other titles
        if job_levels:
            best_level_args = {'levels_to_compare': job_levels, 'levels_ranked': levels_ranked}
            best_level = get_best_level(**best_level_args)

            title_level = best_level
            level_points = get_nomatch_points(level = title_level, level_no_match = levels_not_match)
            points += level_points

        # no job level - just set to NAN
        else:
            title_level = np.nan

        return title_level, new_title, points

# read title matrix
title_matrix = load_xl(root_dir=data_dir, file_name=scoring_file, sheetname='title_matrix')
# list of target titles
target_titles = title_matrix.title.tolist()
# dictionary lookup
d = title_matrix.set_index('title').to_dict('s')

# load spell check mapping
mapping_df = load_xl(root_dir=data_dir, file_name=scoring_file, sheetname='mapping')
mapping = mapping_df.set_index('old').to_dict('s')

# load actual:sfdc mapping
level_mapping_df = load_xl(root_dir=data_dir, file_name=scoring_file, sheetname='level_mapping')
# list of all levels
levels = level_mapping_df.Actual.tolist()
level_mapping = level_mapping_df.set_index('Actual').to_dict()

# load keywords:points mapping
keyword_df = load_xl(root_dir=data_dir, file_name=scoring_file, sheetname='keywords')
keywords = keyword_df.keyword.tolist()
keyword_mapping = keyword_df.set_index('keyword').to_dict()

# load joblevel:points mapping --> for no matches
not_match_df = load_xl(root_dir=data_dir, file_name=scoring_file, sheetname='no_match_levels')
not_match_levels = not_match_df.set_index('job_level').to_dict()

# lists of levels, keywords
levels = sorted(levels, key=len, reverse=True)
keywords = sorted(keywords, key=len, reverse=True)
levels_ordered = not_match_df.sort_values('points', ascending=False).job_level.tolist()

# params for title mapping and score
title_mapping_args = dict(target_title_list = target_titles, level_mapping_dict = level_mapping, points_dict = d,
                          keyword_mapping_dict = keyword_mapping, levels_ranked = levels_ordered, keyword_list = keywords,
                          levels_not_match = not_match_levels)