from functions import  *
from add_titlescore import *

'''
Daily script to populate Job Posting SalesForce object using Beatbox and import.io:
 - scrape jobs from indeed.com for data governance & cio using import.io api
 - clean data:
    - add company url based on google search api
    - add state based on location
    - add title score & determine date posted based on today's date
    - filter title scores >80 and remove posts older than 30 days
'''

#os.chdir("C:\SalesOps\JobPosting")
# out_data_path = os.getcwd() + '/data/output/'

os.chdir('/Users/Gabi/Documents/dev/job_postings/src/')
out_data_path = '/Users/Gabi/Documents/dev/job_postings/data/output/'
jobfilename =  out_data_path + 'jobpostings_uploaded_' + str(dt.today().date()) + '.csv'


# instantiate object & login
# svc = login_beatbox_sandbox()
svc = login_beatbox()
logging.basicConfig(filename='errors.log', level=logging.DEBUG)

# data governance & cio extractor
url_api  = 'https://data.import.io/extractor/f0d09a08-6888-42da-91c6-77aac5e396d7/csv/latest?_apikey=281b13971692444ab021c3bb92ebf79173c7180cde94f96514635468b1048bb6d5089c037f054495212782b710a184d3812527d6fffaa277eceede32176f97f5a77314da48eb97089c7671ca9ed2479d'
content = get_url_content(url_api)

print "\nPutting extractor data in dataframe\n"
# put into DataFrame and clean data
df = pd.read_csv(io.StringIO(content))
print "*" * 25
print "Cleaning DATAZ"
print "*" * 25
data = clean_extractor_data(dat = df)
data = add_title_score(dat= data)
# add State based on location
data['State'] = data.JobLocation.map(lambda x: re.findall('[A-Z][A-Z]', str(x))).map(lambda x: "".join(i for i in x))
data.State.replace('',np.nan, inplace = True)


# titleScore >=80
data = data[data.titleScore.astype(int) >=80]
data = clean_companyLink(dat = data)
data['google_link'] = data.AccountName.map(lambda x: google_results_url(x))


# data.to_csv(out_data_path + "data_results.csv", encoding='utf-8')
df_SF = data.drop('CompanyLink',axis=1).rename(columns={'google_link':'CompanyLink'})


# De-duplicate job_postings - make sure jobId doesnt exist already in JobPosting object
query_jobs = 'Select CreatedDate, JobId__c, Id from JobPosting__c'
jobs_SF = query_data_bb(svc, query_jobs)
jobs_SF_df = pd.DataFrame(jobs_SF)
df_SF = df_SF[~df_SF.JobId.isin(jobs_SF_df.JobId__c)]

print "\n Uploading Dataz to Job Posting SF Object\n"
# upload to SalesForce
sf_create = df_SF.to_dict('r')
try:
    bulk_upload_data(svc, list_of_records= sf_create)
except Exception as e:
    print "ERROR Uploading", e
# print to csv
df_SF.to_csv(jobfilename, index= False)