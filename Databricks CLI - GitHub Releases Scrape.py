# Databricks notebook source
import requests
from datetime import datetime

def fetch_github_releases(owner, repo, token):
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        releases = response.json()
        releases_dict = {}
        prev_release_date = None
        release_times = []
        for release in releases:
            version = release["tag_name"]
            date_str = release["published_at"]
            date = datetime.fromisoformat(date_str.replace('Z', '+00:00')) # Convert ISO8601 format to datetime object
            changelog = release["body"]
            releases_dict[version] = {"date": date, "changelog": changelog}
            if prev_release_date:
                time_diff = date - prev_release_date
                release_times.append(time_diff.days) # Append the time difference in days
            prev_release_date = date
        # Calculate average release time
        average_release_time = sum(release_times) / len(release_times) if release_times else 0
        return releases_dict, average_release_time
    else:
        print(f"Failed to fetch releases. Status code: {response.status_code}")
        return None, None


# COMMAND ----------

from datetime import datetime

# Example usage
owner = "databricks"
repo = "cli"
token = dbutils.secrets.get(scope="github", key="token")

releases, average_release_time = fetch_github_releases(owner, repo, token)
if releases:
    for version, info in releases.items():
        print(f"Version: {version}")
        print(f"Date: {info['date']}")
        # print(f"Changelog:\n{info['changelog']}\n")
    print(f"Average release time between versions: {average_release_time} days")

