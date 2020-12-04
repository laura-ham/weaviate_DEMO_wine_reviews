#!/usr/local/bin/python3
# coding: utf-8

# # Import schema and data to Weaviate

# In[1]:


import json
import pandas as pd
import helper


# ## Initialize the client

# In[2]:


import weaviate


client = weaviate.Client("http://localhost:8080")


df = pd.read_csv('data/preprocessed_data.csv', index_col=0)



# #### Add countries

# In[5]:


def add_countries():
    batch = weaviate.ThingsBatchRequest()
    no_items_in_batch = 0

    # get all the unique country names
    country_names = df.country.dropna().unique()

    # loop through the unique country names and add them to the batch
    for country_name in country_names:
        uuid = helper.generate_uuid('Country', country_name)

        country_object = {
            "name": country_name
        }

        batch.add_thing(country_object, "Country", uuid)
    
    client.batch.create_things(batch)



# In[6]:


def add_provinces():
    batch = weaviate.ThingsBatchRequest()
    refbatch = weaviate.ReferenceBatchRequest()

    # get all the unique province names with the countries
    group = df.groupby('province')
    df2 = group.apply(lambda x: x['country'].unique())
    df2 = df2.apply(pd.Series)
    df2.reset_index(inplace=True)
    df2.columns = ['province', 'country']
    df2.dropna(inplace=True)
    
    # province_names = df2.province.dropna().unique()
    # country_names = df2.country.dropna().unique()

    # loop through the unique provinces, get country reference and add to batch
    for index, row in df2.iterrows():
        province_name = row['province']
        country_name = row['country']
        
        province_uuid = helper.generate_uuid('Province', province_name)
        country_uuid = helper.generate_uuid('Country', country_name)
        
        province_object = {
            "name": province_name
        }

        batch.add_thing(province_object, "Province", province_uuid)
        refbatch.add_reference(province_uuid, "Province", "inCountry", country_uuid)
    
    client.batch.create_things(batch)
    client.batch.add_references(refbatch)



# In[7]:


def add_provinces():
    batch = weaviate.ThingsBatchRequest()
    refbatch = weaviate.ReferenceBatchRequest()

    # get all the unique province names with the countries
    group = df.groupby('province')
    df2 = group.apply(lambda x: x['country'].unique())
    df2 = df2.apply(pd.Series)
    df2.reset_index(inplace=True)
    df2.columns = ['province', 'country']
    df2.dropna(inplace=True)
    
    # loop through the unique provinces, get country reference and add to batch
    for index, row in df2.iterrows():
        province_name = row['province']
        country_name = row['country']
        
        province_uuid = helper.generate_uuid('Province', province_name)
        country_uuid = helper.generate_uuid('Country', country_name)
        
        province_object = {
            "name": province_name
        }

        batch.add_thing(province_object, "Province", province_uuid)
        refbatch.add_reference(province_uuid, "Province", "inCountry", country_uuid)
    
    client.batch.create_things(batch)
    client.batch.add_references(refbatch)



# In[8]:


def add_varieties():
    batch = weaviate.ThingsBatchRequest()

    # get all the unique variety names
    variety_names = df.variety.dropna().unique()

    # loop through the unique country names and add them to the batch
    for variety_name in variety_names:
        uuid = helper.generate_uuid('Variety', variety_name)

        variety_object = {
            "name": variety_name
        }

        batch.add_thing(variety_object, "Variety", uuid)
    
    client.batch.create_things(batch)



# In[10]:


def add_wines(max_items):
    batch_size = 512
    batch = weaviate.ThingsBatchRequest()
    no_items_in_batch = 0
    refbatch = weaviate.ReferenceBatchRequest()
    no_items_in_refbatch = 0

    df2 = df[df['title'].notna()]
    df2 = df2[df2['description'].notna()]
    
    imported_items = 0
    for index, row in df2.iterrows():
        if imported_items > max_items:
            if no_items_in_batch > 0:
                client.batch.create_things(batch)
            if no_items_in_refbatch > 0:
                client.batch.add_references(refbatch)
            return
        
        wine_object = {
            "title": row["title"],
            "description": row["description"],
        }
        
        if not pd.isnull(row['designation']):
            wine_object['designation'] = row['designation']
        if not pd.isnull(row['points']):
            wine_object['points'] = int(row['points'])
        if not pd.isnull(row['price']):
            wine_object['price'] = float(row['price'])
        
        
        wine_uuid = helper.generate_uuid('wine', row["title"]+row["description"])
            
        batch.add_thing(wine_object, "Wine", wine_uuid)
        no_items_in_batch += 1
        
        # add ref to country
        if not pd.isnull(row['country']):
            country_name = row['country']
            country_uuid = helper.generate_uuid('Country', country_name)
            refbatch.add_reference(wine_uuid, "Wine", "fromCountry", country_uuid)
            no_items_in_refbatch += 1

        # add ref to province
        if not pd.isnull(row['province']):
            province_name = row['province']
            province_uuid = helper.generate_uuid('Province', province_name)
            refbatch.add_reference(wine_uuid, "Wine", "fromProvince", province_uuid)
            no_items_in_refbatch += 1

        # add ref to variety
        if not pd.isnull(row['variety']):
            variety_name = row['variety']
            variety_uuid = helper.generate_uuid('Variety', variety_name)
            refbatch.add_reference(wine_uuid, "Wine", "hasVariety", variety_uuid)
            no_items_in_refbatch += 1

        if no_items_in_batch >= batch_size:
            results = client.batch.create_things(batch)
            # for result in results:
            #     if result['result'] != {}:
            #         helper.log(result['result'])

            batch = weaviate.ThingsBatchRequest()
            imported_items += no_items_in_batch
            no_items_in_batch = 0

        if no_items_in_refbatch >= batch_size:
            results = client.batch.add_references(refbatch)
            # for result in results:
            #     if result['result']['status'] != 'SUCCESS':
            #         helper.log(result['result'])

            refbatch = weaviate.ReferenceBatchRequest()
            no_items_in_refbatch = 0
            
    client.batch.create_things(batch)
    client.batch.add_references(refbatch)


def run():
    for i in range(1000):
        client.schema.delete_all()
        schema = 'schema.json'
        client.schema.create(schema)

        add_countries()
        add_provinces()
        add_provinces()
        add_varieties()

        add_wines(1024)

        query = """
          {
            Get {
              Things {
                Wine(explore: {
                  concepts: ["pizza"]
                }) {
                  description
                }
              }
            }
          }
        """

        query_result = client.query.raw(query)
        no_of_results = len(query_result["data"]["Get"]["Things"]["Wine"])
        print("Round {}: Result length {} / 100".format(i, no_of_results))
        if no_of_results != 100:
            exit(1)


run()
