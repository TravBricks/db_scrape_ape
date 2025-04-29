# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Runtime - Release, support, spark, variants
# MAGIC Scrape Databricks release details of Databricks Runtimes (DBR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

import requests

def get_runtime_release_details(url):
    # Send a GET request to the URL
    response = requests.get(url)
    
    releases = []
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table element
        tables = soup.find_all('table')

        release_details = []

        for table in tables:
            # Find tables containing a certain header cell
            if table.find('th', text='End-of-support date'):
              release_details.extend(get_release_details(table))              
            else:
              #probably not what we're looking for
              continue
        return release_details
    else:
        # If the request was unsuccessful, print an error message
        print("Error: Unable to retrieve data. Status code:", response.status_code)
        return None


def get_release_details(table):
  releases = []

  # Extract all details from the runtime listed table
  for row in table.find_all('tr')[1:]:  # Skip the header row
      cells = row.find_all('td')
      
      variants = []
      for li in cells[1]:
          a_tag = li.find("a")
          if a_tag:
              variants.append({'link_text':a_tag.get_text()
                              ,'link_href':a_tag.get("href") })

      releases.append( {
          'release': cells[0].text.strip()
          ,'variants': variants
          ,'spark_version': cells[2].text.strip()
          ,'release_date': cells[3].text.strip()
          ,'support_date': cells[4].text.strip()
      })
        
  return releases

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main program

# COMMAND ----------


release_url_base = "https://learn.microsoft.com"
release_notes_page = f"{release_url_base}/en-us/azure/databricks/release-notes/runtime/"

# Collect the release summary from the release notes page
runtimes_supported = get_runtime_release_details(release_notes_page)

from pprint import pprint as pp

pp(runtimes_supported)


# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, current_date, regexp_replace

df = spark.createDataFrame(runtimes_supported)
df = (df.dropDuplicates(["release"])
      #handle typo
      .withColumn("release_date", regexp_replace(col("release_date"), "June", "Jun"))
      .withColumn("support_date", regexp_replace(col("support_date"), "June", "Jun"))
      
      #convert to date
      .withColumn("release_date", to_date(col("release_date"), "MMM d, yyyy"))
      .withColumn("support_date", to_date(col("support_date"), "MMM d, yyyy"))
      
      #calc remaining days
      .withColumn("remaining_support_days", datediff(col("support_date"), current_date()))
     )
display(df)
