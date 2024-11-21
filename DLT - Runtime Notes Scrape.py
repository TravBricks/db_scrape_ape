# Databricks notebook source
# MAGIC %pip install beautifulsoup4

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def extract_links(url):
    # Request the HTML content of the page
    response = requests.get(url)
    response.raise_for_status()  # Check for successful request

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the div with the specified ID
    target_div = soup.find('div', id='delta-live-tables-release-notes')
    
    # Initialize an empty list to hold link dictionaries
    links = []
    
    if target_div:
        # Find all links within <ul> tags inside the target div
        ul = target_div.find('ul')
        if ul:
            for li in ul.find_all('li'):
                a_tag = li.find('a')
                if a_tag and a_tag['href']:
                    # Append the URL and link text to the list
                    links.append({
                        'text': a_tag.get_text(strip=True),
                        'url': a_tag['href']
                    })

    return links
  

def retrieve_list_content(base_url, link_data):
  all_content = {}

  for link in link_data:
      release_link = urljoin(base_url, link['url'])
      release_version = link['text']
      response = requests.get(release_link)
      response.raise_for_status()
      soup = BeautifulSoup(response.text, 'html.parser')

      target_div = soup.find('div', id='new-features-and-improvements-in-this-release')
      page_content = []

      if target_div:
          ul = target_div.find('ul')
          if ul:
              for li in ul.find_all('li'):
                  p_tag = li.find('p')
                  if p_tag:
                      page_content.append(p_tag.get_text(strip=True))

      all_content[release_version] = page_content

  return all_content

def generate_html(all_content):
    html_content = "<html><body><h1>Delta Live Tables - Release Notes</h1><ul>"

    for release_name, notes in all_content.items():
        html_content += f"<li><b>{release_name}</b><ul>"
        if not notes:
          notes = ["Bug fixes"]
        for note in notes:
            html_content += f"<li><p>{note}</p></li>"
        html_content += "</ul></li>"

    html_content += "</ul></body></html>"
    return html_content

# COMMAND ----------


# Get list of releases and release note links
base_url = "https://docs.databricks.com/en/release-notes/delta-live-tables/"
release_note_list_url = f"{base_url}index.html"
link_data = extract_links(release_note_list_url)
# print(link_data)

# Get list of releases and release note links
content_data = retrieve_list_content(base_url, link_data)

# Print out the results in a nice single list
displayHTML(generate_html(content_data))
