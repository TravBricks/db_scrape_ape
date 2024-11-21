# Databricks notebook source
import requests
from bs4 import BeautifulSoup

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
        variants_column_index = headers.index("Variants")
        
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

# COMMAND ----------

def scrape_libraries_from_release_notes(url, div_id):
    # Send a GET request to the URL
    response = requests.get(url)
    
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


# COMMAND ----------


release_url_base = "https://docs.databricks.com/en/release-notes/runtime/"
release_notes_page = f"{release_url_base}index.html"
release_note_links = get_release_note_links_from_variants_column(release_notes_page)

div_ids = ["installed-python-libraries","python-libraries-on-cpu-clusters","python-libraries-on-gpu-clusters"]

release_libs = []
for link in release_note_links:
  full_link = f"{release_url_base}{link}"
  for div in div_ids:
    libs = scrape_libraries_from_release_notes(full_link, div)
    if libs:
      release_libs.append({'link':link
                           , 'section':div
                           , 'libraries':libs})

# COMMAND ----------

from pprint import pprint as pp

pp(release_libs)

# COMMAND ----------

from pyspark.sql.functions import explode, col, first, regexp_extract, contains, when, concat, lit

df = spark.createDataFrame(release_libs)
df = (df.withColumn("library_sub", explode(df["libraries"]))
        .withColumn("dbr", regexp_extract(df["link"], "(((\d{1,2})\.(\d{1,2}))(lts)?(\-ml)?)", 0))
        .withColumn("is_gpu", when(df["section"].contains("gpu"),True).otherwise(False))
        .withColumn("dbr_version",
          when(df["section"].contains("gpu")
                ,concat(col("dbr"),lit("-gpu"))
              ).otherwise(col("dbr")))
        .select("dbr_version","dbr","library_sub.library","library_sub.version")
  )
display(df)

# COMMAND ----------


df_pivot = df.groupBy("library").pivot("dbr_version").agg(first("version")).orderBy('library')

display(df_pivot)
