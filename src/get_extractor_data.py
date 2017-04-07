
from functions import  *
from add_titlescore import *
from random import  randint

os.chdir('/Users/Gabi/Documents/dev/job_postings')
out_data_path = os.getcwd() + '/data/output/'

# instantiate object & login
svc = login_beatbox_sandbox()
logging.basicConfig(filename='errors.log', level=logging.DEBUG)
# data governance & cio search

# 10 pages of data governance indeed job posts
# url = 'https://data.import.io/extractor/00698f74-6477-4440-88d1-dc1b0102b95a/csv/latest?_apikey=281b13971692444ab021c3bb92ebf79173c7180cde94f96514635468b1048bb6d5089c037f054495212782b710a184d3812527d6fffaa277eceede32176f97f5a77314da48eb97089c7671ca9ed2479d'

# url = 'https://data.import.io/extractor/00698f74-6477-4440-88d1-dc1b0102b95a/csv/latest?_apikey=281b13971692444ab021c3bb92ebf79173c7180cde94f96514635468b1048bb6d5089c037f054495212782b710a184d3812527d6fffaa277eceede32176f97f5a77314da48eb97089c7671ca9ed2479d'

# url_api = 'https://data.import.io/extractor/ead0437c-93b5-453c-99a5-2667ef88ab4d/csv/latest?_apikey=281b13971692444ab021c3bb92ebf79173c7180cde94f96514635468b1048bb6d5089c037f054495212782b710a184d3812527d6fffaa277eceede32176f97f5a77314da48eb97089c7671ca9ed2479d'
url_api  = 'https://data.import.io/extractor/f0d09a08-6888-42da-91c6-77aac5e396d7/csv/latest?_apikey=281b13971692444ab021c3bb92ebf79173c7180cde94f96514635468b1048bb6d5089c037f054495212782b710a184d3812527d6fffaa277eceede32176f97f5a77314da48eb97089c7671ca9ed2479d'
content = get_url_content(url_api)
df = pd.read_csv(io.StringIO(content))

data = clean_dat(dat = df)
data = add_title_score(dat= data)
data = data[data.titleScore.astype(int) >=80]
data = clean_companyLink(dat = data)

# upload to salesforce
sf_create = data.to_dict('r')
bulk_upload_data(svc, list_of_records= sf_create)


# company xpath link
'//div[@id="cmp-sidebar"]/dl[@id="cmp-company-details-sidebar"]/dd/a'


comp = data.CompanyLink.ix[3]
comp_text = get_url_content(comp)
root = html.fromstring(comp_text)
sidebar = root.find_class("cmp-dl-list-big cmp-sidebar-section cmp-bordered-box")

sidebar = root.find_class("cmp-inline-block")
sidebar[0].xpath('dd/a')[0].attrib['href']



def google_results_url(query):
    api_dir = '/Users/Gabi/Documents/dev/api_dir/'
    # api key
    api_key = open('/Users/Gabi/Documents/dev/api_dir/knowledge_graph.api_key').read()
    service = build("customsearch", "v1",
                    developerKey=api_key)
    result = service.cse().list(
            q=query,
            cx='014731159833071467066:htqxou8lqhw'
        ).execute()

    return result["items"][0]["formattedUrl"]


dat_2 = clean_dat(dat = df)
dat_2 = add_title_score(dat= dat_2)
# <30 days ago & titlescore >=80
dat_2 = dat_2[dat_2.titleScore.astype(int) >=80]
dat_2 = clean_companyLink(dat = dat_2)
dat_2['google_link'] = dat_2.AccountName.map(lambda x: google_results_url(x))

div_list = soup.find_all('div')
h2_tags = [h for h in div_list if h.find_all('h2')]
google_url = soup.find_all('h2')[0].contents[0]


def get_link(comp, dat):
    acct = dat[dat.CompanyLink == comp].AccountName.iloc[0]
    google_url = "https://www.google.com/#q=" + acct

    # if indeed url is google link - wont find it
    if is_google_url(comp):
        return comp
    # otherwise try and find
    else:
        soup = BeautifulSoup(urllib.urlopen(comp))
        dd_list =  soup.find_all('dd')
        a_tags = [row for row in dd_list if row.find_all('a')]
        # company url is in a_tag
        if a_tags:
            links = [[j['href'] for j in i.find_all('a')] for i in a_tags]
            links = list(chain.from_iterable(links))
            # if need to claim company profile build google url then company link isnt there
            #claim_page = re.findall('^/.*(claim)/.*', links[0])

            links = list(chain.from_iterable([re.findall("http.*|https.*.com|www.*.com", i) for i in links]))
            if links:
                return  links
            # elif claim_page:
            #         return google_url
            else:
                return google_url
        else:
            return  google_url


job_url = data.ix[1].JobLink
job_content = get_url_content(job_url)
job_tree = html.fromstring(job_content)

job_text_el = job_tree.xpath('//p|//li')
# job_text_el = job_tree.xpath('//p[@class="links"]|//p[@class=""]|//li')
text = [" ".join(i.text_content().split()) for i in job_text_el]
try:
    text = [s.decode('utf-8') for s in text]
    text = [s for s in text if isinstance(s, unicode)]
except:
    text = [s for s in text if isinstance(s, unicode)]



## TODO: parse through content
job_text_el[0].tag
job_text_el[0].text_content()




job_soup = BeautifulSoup(job_html, 'html.parser')
job_soup.find_all(id = 'article')

job_root = html.fromstring(job_url)



def test(data):
    # testing out function
    comp = sample_links(data)
    compName = data[data.CompanyLink == comp].AccountName.tolist()[0]
    compName = compName.replace(" ","").lower()
    print "Company name + link", compName, " ", comp
    link = get_link(comp,dat=data)
    print  "link found", link
    while len(link) !=1:
        test(data)
    print fuzz_best_match(companyName = compName, companyLink=link, matching_function = 'token_set_ratio')


# add Company Links scrapped from indeed.com/companyname
data['links']= data.CompanyLink.map(lambda x : get_link(x))
data.loc[data.links.map(lambda x: len(x)==1),'links'] = data[data.links.map(lambda x: len(x)==1)].links.map(lambda x: x[0])
# if nothing is on page, return google.com/#q=companyname
data.loc[data.links.map(lambda x: len(x)==0),'links'] = data[data.links.map(lambda x: len(x)==1)].links.map(lambda x: x[0])

for i in xrange(0, 5):
    print i,"th sample"
    comp = sample_links(data)
    print "\n company link on indeed: ", comp
    link = get_link(comp,data)
    print "\n link based on indeed scrapping: ", link


# xpath for google resutls:
# '//div[@class="rc"]/h3/a[@href]'

#     if len(a_tags) >1:
#         links = [j['href'] for j in i.find_all('a') for i in a_tags]
#     else:
#         links = [i.find_all('a')[0].get('href') for i in a_tags]
#
#     for link in links:
#         if re.findall('www.*', link):
#             comp_link = re.findall('www.*', link)
#             return  comp_link
#
#     [" ".join(j for j in re.findall("www.*",i)) if re.findall("www.*",i) else None for i in links]
#     for row in dd_list:
#         a_tag_list = row.find_all('a')
#         for a_tag in a_tag_list:
#             if a_tag:
#                 print a_tag
#                 link = a_tag.get('href')
#                 print "link", link
#                 comp_link = re.findall('www.*', link)
#                 return comp_link
#             else:
#                 print "a tag"
#
#
# for d in soup.find_all('dd'):
#     print d.find_all('a')[0]['href']


# to delete records in test
s = 'select Id from JobPosting__c'
res = query_data_bb(svc, s)
del_df = pd.DataFrame(res)
for rec in del_df.Id:
    print "deleting id:", rec
    svc.delete(rec)


