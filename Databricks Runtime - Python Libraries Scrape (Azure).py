# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks runtime - Python Libraries
# MAGIC
# MAGIC Scrape Databricks docs for all python libraries and versions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initial setup

# COMMAND ----------

catalog = "uc_travislongwell"
schema = "scrape_ape"
volume = "docs"
folder = "dbr"
docs_path = f"/Volumes/{catalog}/{schema}/{volume}/{folder}"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
_ = dbutils.fs.mkdirs(docs_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

import requests
from bs4 import BeautifulSoup

def file_exists(full_path):
    # Check if the file exists in the specified path
    try:
        dbutils.fs.ls(full_path)
        return True
    except Exception as e:
        return False

def download_release_page(url, full_path):
    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Write the content of the response to a file
        with open(full_path, "wb") as file:
            file.write(response.content)
        
        return True
    else:
        # Print an error message if the request was unsuccessful
        print("Error: Unable to download the file. Status code:", response.status_code)
        return False

def release_note_page(url, docs_path):
    # Extract the file name from the URL and create the full path
    file_name = (url.split("/")[-1] + ".html") #.replace('-','_')

    full_path = f"{docs_path}/{file_name}"

    # Check if the file already exists
    if not file_exists(full_path=full_path):
        # Download the release note page if the file does not exist
        if download_release_page(url=url, full_path=full_path):
            print(f"Release note file {file_name} downloaded")

    return True

def get_release_note_links_from_variants_column(url):
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table element
        table = soup.find('table')
        
        # Find the index of the "Variants" column
        header_row = table.find('tr')
        headers = [header.text.strip() for header in header_row.find_all('th')]

        variants_column_index = 1
    
        # Find all links (a tags) within the "Variants" column cells
        release_note_links = []
        for row in table.find_all('tr')[1:]:  # Skip the header row
            cells = row.find_all('td')
            link_cell = cells[variants_column_index]
           
            links = link_cell.find_all('a')
            for link in links:
                release_note_links.append(link['href'])
        
        return release_note_links
        
    else:
        # If the request was unsuccessful, print an error message
        print("Error: Unable to retrieve data. Status code:", response.status_code)
        return None
    
def scrape_libraries_from_release_notes(url, div_id):
    # Send a GET request to the URL
    response = requests.get(url, allow_redirects=True)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')
        
        try:
          # Find the div element with the specified id
          div_element = soup.find('div', id=div_id)
          
          # Find the table within the div element
          table = div_element.find('table')
          
          # Extract data from each row of the table
          table_data = []
          for row in table.find_all('tr'):
              row_data = []
              for cell in row.find_all(['th', 'td']):
                  row_data.append(cell.text.strip())
              if row_data:
                  table_data.append(row_data)
          
          # Extract library and version information from the table data
          libraries = []
          for row in table_data[1:]:
            # each row has 3 sets of library and version, extracting by index
            libraries.append({'library': row[0], 'version': row[1]})
            if row[2] != '':
              libraries.append({'library': row[2], 'version': row[3]})
            if row[4] != '':
              libraries.append({'library': row[4], 'version': row[5]})
              
          return libraries
        except:
          return None        
    else:
        # If the request was unsuccessful, print an error message
        print("Error: Unable to retrieve data. Status code:", response.status_code)
        return None

from bs4 import BeautifulSoup

def locate_process_library_tables(html_doc, id_list):
    # Parse the HTML document
    soup = BeautifulSoup(html_doc, 'html.parser')
    
    results = {}
    
    elements = ['h3', 'h4']
    for target_id in id_list:
        # some are H3 and some are H4, so we need to check both
        for e in elements:
          title_element = soup.find(e, {'id': target_id})
          if title_element:
              table = title_element.find_next('table')
              if table:
                results[target_id] = library_table_extract(table)
        
    return results

def library_table_extract(table):
  # Extract data from each row of the table
  table_data = []
  for row in table.find_all('tr'):
    row_data = []
    for cell in row.find_all(['th', 'td']):
        row_data.append(cell.text.strip())
    if row_data:
        table_data.append(row_data)

  # Extract library and version information from the table data
  libraries = []
  for row in table_data[1:]:
    # each row has 3 sets of library and version, extracting by index
    libraries.append({'library': row[0], 'version': row[1]})
    if row[2] != '':
      libraries.append({'library': row[2], 'version': row[3]})
    if row[4] != '':
      libraries.append({'library': row[4], 'version': row[5]})
    
  return libraries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main program

# COMMAND ----------

import time

release_url_base = "https://learn.microsoft.com"
release_notes_page = f"{release_url_base}/en-us/azure/databricks/release-notes/runtime/"

# Get the list of release note links from the variants column
release_note_links = get_release_note_links_from_variants_column(release_notes_page)

# List of div IDs to look for in the release note pages
div_ids = ["installed-python-libraries", "python-libraries-on-cpu-clusters", "python-libraries-on-gpu-clusters"]

if release_note_links:
  release_libs = []
  for link in release_note_links:
    # Construct the full URL for each release note link
    full_link = f"{release_notes_page}{link}"

    # Download the release note page if it does not already exist
    if not release_note_page(url=full_link, docs_path=docs_path):
      raise Exception("Failed to download release note page")
  
  print(f"Release notes have been downloaded to '{docs_path}'")

else:
  print("No release notes found")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Show docs files in volume

# COMMAND ----------

#Show what we have in the 
docs_files = dbutils.fs.ls(docs_path)
display(docs_files)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parse docs files

# COMMAND ----------

runtime_libraries = {}

for file in docs_files:
  joint_path = f"{docs_path}/{file.name}"
  dbr_version = file.name.replace('.html','')
  # print(f"{joint_path=}")

  with open(joint_path, 'r') as file:
      html_doc = file.read()

  target_ids = ["installed-python-libraries", "python-libraries-on-cpu-clusters", "python-libraries-on-gpu-clusters"]
  runtime_libraries[dbr_version] = locate_process_library_tables(html_doc=html_doc, id_list=target_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flatten the parsed data into a dataframe

# COMMAND ----------

flattened_runtime_libraries = []

for dbr_version, libraries in runtime_libraries.items():
    for section, libs in libraries.items():
        for lib in libs:
            flattened_runtime_libraries.append({
                'dbr_version': dbr_version,
                'section': section,
                'library': lib['library'],
                'version': lib['version']
            })

df_flattened = spark.createDataFrame(flattened_runtime_libraries)
display(df_flattened.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Augment the data, strip down for pivot

# COMMAND ----------

from pyspark.sql.functions import explode, col, first, regexp_extract, contains, when, concat, lit

df_augmented = (df_flattened
                  .withColumn("dbr", regexp_extract(df_flattened["dbr_version"], "(((\d{1,2})\.(\d{1,2}))(lts)?(\-ml)?)", 0))
                  .withColumn("is_gpu", when(df_flattened["section"].contains("gpu"),True).otherwise(False))
                  .withColumn("dbr_version",
                    when(df_flattened["section"].contains("gpu")
                          ,concat(col("dbr"),lit("-gpu"))
                        ).otherwise(col("dbr")))
                  .select("dbr_version","dbr","library","version")
                  .sort("dbr_version", "library", "is_gpu")
                )

display(df_augmented.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pivot and display results

# COMMAND ----------


df_pivot = df_augmented.groupBy("library").pivot("dbr_version").agg(first("version")).orderBy('library')

display(df_pivot)
