#!/usr/bin/env python
# coding: utf-8

# # Exercise 3: Parallel ETL

# In[1]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[2]:


import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time


# # STEP 1: Get the params of the created redshift cluster 
# - We need:
#     - The redshift cluster <font color='red'>endpoint</font>
#     - The <font color='red'>IAM role ARN</font> that give access to Redshift to read from S3

# In[11]:


#load DWH Params from a file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY=config.get('AWS','KEY')
SECRET= config.get('AWS','SECRET')

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")


# In[12]:


# FILL IN THE REDSHIFT ENPOINT HERE
# e.g. DWH_ENDPOINT="redshift-cluster-1.csmamz5zxmle.us-west-2.redshift.amazonaws.com" 
DWH_ENDPOINT="dwhcluster.c8usstvcpcm4.us-west-2.redshift.amazonaws.com" 
    
#FILL IN THE IAM ROLE ARN you got in step 2.2 of the previous exercise
#e.g DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
DWH_ROLE_ARN="arn:aws:iam::107743668363:role/dwhRole"


# In[13]:


print("KEY :: ", KEY)
print("SECRET :: ", SECRET)
print("DWH_DB_USER :: ", DWH_DB_USER)
print("DWH_DB_PASSWORD :: ", DWH_DB_PASSWORD)
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


# # STEP 2: Connect to the Redshift Cluster

# In[6]:


conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
get_ipython().run_line_magic('sql', '$conn_string')


# In[14]:


# Create S3 cient
s3 = boto3.resource('s3',
                    region_name="us-west-2", 
                    aws_access_key_id=KEY, 
                    aws_secret_access_key=SECRET
                   )

# Create udacity-labs bucket
sampleDbBucket = s3.Bucket("udacity-labs")


# In[15]:


for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)


# # STEP 3: Create Tables

# In[16]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS "sporting_event_ticket";\nCREATE TABLE "sporting_event_ticket" (\n    "id" double precision DEFAULT nextval(\'sporting_event_ticket_seq\') NOT NULL,\n    "sporting_event_id" double precision NOT NULL,\n    "sport_location_id" double precision NOT NULL,\n    "seat_level" numeric(1,0) NOT NULL,\n    "seat_section" character varying(15) NOT NULL,\n    "seat_row" character varying(10) NOT NULL,\n    "seat" character varying(10) NOT NULL,\n    "ticketholder_id" double precision,\n    "ticket_price" numeric(8,2) NOT NULL\n);')


# # STEP 4: Load Partitioned data into the cluster
# Use the COPY command to load data from `s3://udacity-labs/tickets/split/part` using your iam role credentials. Use gzip delimiter `;`.

# In[17]:


get_ipython().run_cell_magic('time', '', 'qry = """\n    COPY sporting_event_ticket FROM \'s3://udacity-labs/tickets/split/part\'\n    credentials \'aws_iam_role={}\'\n    gzip delimiter \';\' compupdate off region \'us-west-2\';\n""".format(DWH_ROLE_ARN)\n\n%sql $qry')


# # STEP 5: Create Tables for the non-partitioned data

# In[18]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS "sporting_event_ticket_full";\nCREATE TABLE "sporting_event_ticket_full" (\n    "id" double precision DEFAULT nextval(\'sporting_event_ticket_seq\') NOT NULL,\n    "sporting_event_id" double precision NOT NULL,\n    "sport_location_id" double precision NOT NULL,\n    "seat_level" numeric(1,0) NOT NULL,\n    "seat_section" character varying(15) NOT NULL,\n    "seat_row" character varying(10) NOT NULL,\n    "seat" character varying(10) NOT NULL,\n    "ticketholder_id" double precision,\n    "ticket_price" numeric(8,2) NOT NULL\n);')


# # STEP 6: Load non-partitioned data into the cluster
# Use the COPY command to load data from `s3://udacity-labs/tickets/full/full.csv.gz` using your iam role credentials. Use gzip delimiter `;`.
# 
# - Note how it's slower than loading partitioned data

# In[19]:


get_ipython().run_cell_magic('time', '', '\nqry = """\n    COPY sporting_event_ticket_full FROM \'s3://udacity-labs/tickets/full/full.csv.gz\'\n    credentials \'aws_iam_role={}\'\n    gzip delimiter \';\' compupdate off region \'us-west-2\';\n""".format(DWH_ROLE_ARN)\n\n%sql $qry')


# In[ ]:




