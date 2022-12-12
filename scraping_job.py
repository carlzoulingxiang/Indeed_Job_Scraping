from bs4 import BeautifulSoup
import requests
import csv
from time import sleep
from random import randint
from datetime import datetime
import re
import pandas as pd
import warnings
from pathlib import Path, PureWindowsPath
import os


def get_url(position, location):
    """Generate url from position and location"""
    template = 'http://ca.indeed.com/jobs?q={}&l={}&fromage=1'
    position = position.replace(' ', '+')
    location = location.replace(' ', '+')
    url = template.format(position, location)
    return url

def get_record(card):
    job_title = card.find('h2',re.compile('jobTitle.*')).find_all('span')[-1].text
    location = card.find('div','companyLocation').text
    company_name = card.find('span','companyName').text
    try: 
        job_type = card.find_all('svg', {"aria-label": "Job type"})[0].parent.text
        job_type = job_type.split(' ')[0]
    except IndexError:
        job_type = None

    
    date = datetime.today().strftime('%Y-%m-%d')
    summary = card.find('div','job-snippet').text.strip().split('\n')
    try: 
        link = card.find_all('h2', {'class': 'jobTitle jobTitle-newJob css-bdjp2m eu4oa1w0'})[0].find('a')
        job_url = 'http://ca.indeed.com'+ link.get('href')
        job_response = requests.get(job_url,headers=headers)
        job_soup = BeautifulSoup(job_response.text, 'html.parser')
    except IndexError:
        pass
    

    try:
        string = job_soup.find('div',{"class": 'jobsearch-jobDescriptionText'}).text.lower()
    except AttributeError:
        string = ""
    list_keywords = re.sub(r"[^a-zA-Z0-9]+", ' ', string ).split()
    tools = list(set(list_keywords)&set(keywords_programming))
    
    num = job_soup.text.find('years')
    if num != -1:
        experience = job_soup.text[num-6:num+5]
    else:
        experience = None
    record = [job_title, company_name, job_type, location, date, tools, experience, summary, job_url]
    
    return record

def main(position, location):
    """Run the main program routine"""
    headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Vivaldi/5.3.2679.70.'}
    records = []
    url = get_url(position, location)
    column_names = ['job_title', 'company_name', 'job_type', 'location', 'date', 'tools', 'experience', 'summary', 'job_url']
    df = pd.DataFrame(columns=column_names)
    # extract the job data
    next_url = ''
    while True:
        response = requests.get(url,headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        regex = re.compile('tapItem fs-unmask.*')
        cards = soup.find_all("div", {"class": regex})
        
        
        
        for i in range(len(cards)):
            a_series = pd.Series(get_record(cards[i]), index = df.columns)
            df = df.append(a_series, ignore_index=True)

        for link in soup.find_all('a', {'aria-label': 'Next Page'}):
            next_url = 'http://ca.indeed.com' + link.get('href')
    
        if next_url == url:
            break
        else:
            url = next_url
        
        # try:
        #     for link in soup.find_all('a', {'aria-label': 'Next Page'}):
        #         url = 'http://ca.indeed.com' + link.get('href')
        #     print(url)
        # except AttributeError:
        #     break
        # filename = 'job' + datetime.today().strftime('%Y-%m-%d')
        # path = Path(r'/Users/carl/Desktop/UBC/DATA552/Project/', filename+'.csv')
        
    return df
        
warnings.simplefilter(action='ignore', category=FutureWarning)


headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Vivaldi/5.3.2679.70.'}
# Skill 
keywords_programming = [
'sql', 'python', 'r', 'c', 'c#', 'javascript', 'js',  'java', 'scala', 'sas', 'matlab', 
'c++', 'c/c++', 'perl', 'go', 'typescript', 'bash', 'html', 'css', 'php', 'powershell', 'rust',
'ruby', 'vba', 'julia', 'javascript/typescript', 'golang', 'nosql', 'mongodb', 't-sql', 'no-sql',
'visual_basic', 'pascal', 'mongo', 'pl/sql',  'sass', 'vb.net', 'mssql', 
'excel','tableau','word', 'powerpoint', 'looker', 'powerbi',
'outlook' , 'azure', 'jira', 'twilio', 'shell','linux','sas', 'sharepoint','mysql', 'visio', 'git','mssql',
'powerpoints', 'postgresql', 'seaborn', 'pandas', 'gdpr', 'spreadsheet', 'alteryx', 'aws', 'gcp','google cloud',
'github', 'postgres', 'ssis','numpy', 'dplyr', 'tidyr', 'ggplot2',
'plotly', 'docker', 'linux','jira','hadoop','airflow', 'sap', 'tensorflow','jquery',
'pyspark', 'pytorch', 'gitlab','selenium', 'splunk', 'bitbucket','qlik','terminal','linux/unix', 'ubuntu']

df = main('data analysis', 'Canada')
df['tools'] = [','.join(map(str, l)) for l in df['tools']]
df['summary'] = [','.join(map(str, l)) for l in df['summary']]
df['date'] = pd.to_datetime(df['date'])

import os
from pandas.io import gbq
from google.cloud import bigquery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=os.getcwd()+'/automation/Indeed_Job_Scraping/'+'scraping-project-370223-320e11f2280c.json'

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = 'scraping-project-370223.Indeed_data.jobs'

job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("job_title", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('company_name', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('job_type', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("location", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('tools', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('experience', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('summary', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('date', bigquery.enums.SqlTypeNames.DATE),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_APPEND",
)

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)