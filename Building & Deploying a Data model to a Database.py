#!/usr/bin/env python
# coding: utf-8

# In[4]:


# !pip install psycopg2
# !pip install pandas 

# you first have to install these packages if you dont already have them, I already have them so i wont install again

import psycopg2 # to connect to our database server
import pandas as pd # to read, manipulate, upload the data


# In[3]:


# now we will create a function to create our database
def create_database():
    try:
        conn = psycopg2.connect("host = localhost dbname = postgres user = postgres password = root150") #connecting to the default database
    except psycopg2.Error as e :
        print("error, couldnt connect to database")   
        print(e)
    conn.set_session(autocommit = True) # any changes we make from here will be updated automatically in the database
    curr = conn.cursor()
    
    curr.execute("Create Database airbnb")   # creating our database in postgres
    conn.close()   # closing connection to the default database
    
    try:
        conn = psycopg2.connect("host = localhost dbname = airbnb user = postgres password = root150") # connecting to our new database
    except psycopg2.Error as e :
        print("error, couldnt connect to database")
        print(e)
    conn.set_session(autocommit = True)
    curr = conn.cursor() # creating a cursor to be able to communicate to our database
    
    return curr, conn


# In[ ]:


# in the next section, we will read all the files and filter all the data that we require


# In[5]:


review_details = pd.read_csv("/Users/ADMIN/Desktop/airbnbdf/reviews_details.csv") # reading the csv file


# In[6]:


review_details.head() # displaying first 5 rows


# In[7]:


reviews = review_details[['id', 'comments']] # copying the desired columns into a separate data frame


# In[8]:


listings_raw = pd.read_csv("/Users/ADMIN/Desktop/airbnbdf/listings.csv") # reading the csv file


# In[9]:


listings_raw.head() # displaying first 5 rows


# In[10]:


listings = listings_raw[['id', 'name', 'room_type', 'price']] # copying the desired columns into a separate data frame


# In[15]:


listing_details_raw = pd.read_csv("/Users/ADMIN/Desktop/airbnbdf/listings_details.csv", low_memory = False) # reading the csv file


# In[16]:


listing_details_raw.head() # displaying first 5 rows


# In[17]:


listing_details = listing_details_raw[['id', 'last_scraped', 'summary', 'description']] # copying the desired columns into a separate data frame


# In[18]:


curr, conn = create_database() # calling the function to create the database


# In[34]:


reviews_create = ("""Create table if not exists reviews(
id int Primary key, 
comments varchar)""") # storing our SQL query to create a table inside a variable


# In[35]:


curr.execute(reviews_create) # Executing our SQL query


# In[23]:


listings_create = ("""Create table if not exists listings(
id int,
name varchar,
room_type varchar,
price int)""")  


# In[24]:


curr.execute(listings_create) 


# In[25]:


listing_details_create = ("""create table if not exists listing_details(
id int, 
last_scraped date,
summary varchar,
description varchar)""") 


# In[26]:


curr.execute(listing_details_create) 


# In[36]:


reviews_insert = ("""insert into reviews(
id,
comments)
values (%s, %s)""") # storing a SQL query to insert data into a table inside a variable


# In[37]:


for i, row in reviews.iterrows(): # iterrows returns index and information stored in a row of a data frame
    curr.execute(reviews_insert, list(row)) # So we are reading each row from reviews dataframe and inserting it into reviews
                                            # table 


# In[30]:


listings_insert = ("""insert into listings(
id,
name,
room_type,
price)
values (%s, %s, %s, %s)""") 


# In[31]:


for i, row in listings.iterrows():
    curr.execute(listings_insert, list(row))


# In[32]:


listing_details_insert = ("""insert into listing_details(
id, 
last_scraped,
summary,
description)
values (%s, %s, %s, %s)
""")


# In[33]:


for i, row in listing_details.iterrows():
    curr.execute(listing_details_insert, list(row))


# In[ ]:




