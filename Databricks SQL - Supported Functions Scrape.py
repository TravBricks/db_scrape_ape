# Databricks notebook source
# MAGIC %md
# MAGIC ## Process raw results from Profiler

# COMMAND ----------

raw_table = """UNSUPPORTED_FUNCTION	RUN_COUNT	TOTAL_CREDITS
try_cast	14,485,541	20,160
obfuscateid	6,341,296	19,862
try_to_double	2,464,911	12,944
sysdate	2,672,593	3,054
format_big_decimal	2,493,972	2,221
uuid_string	19,354	1,231
listagg	366,325	821
last_query_id	31,361,130	777
insert	131,436	708
snow_account	119,297	138
format_big_decimal_real	205	45.466332
get_current_timezone	260	16.032897
udf_java_csv_parser_v2	5681	5.586467
is_serverless_warehouse	35	2.593086
period_range_plus	32	2.593086
get_credit_cost	32	2.593086
qtag	30	2.593086
qtag_to_map	26	2.593086
period_range	22	2.593086
qtag_value	20	2.580912
qtag_matches	16	2.580912
get_serverless_credit_cost	13	2.580912
qtag_sources	12	2.580912
qtag_keys	12	2.580912
qtag_exists	12	2.580912
get_storage_cost	9	2.580912
current_region	1136	1.732766
current_account	1136	1.732766
object_construct	4253	1.305934
current_warehouse	11	0.752674
time_slice	3	0.752674
get_ddl	247	0.618876
array_to_string	68	0.523482
create_snow	68	0.462612
seq4	64	0.462612
timeadd	60	0.462612
div0	60	0.462612
is_weekday	61	0.44638
get_utilization_based_warehouse_size	60	0.44638
get_cost_per_second	44	0.44638
timediff	45	0.443123
quote	99	0.442322
to_time	54	0.442322
warehouse_size_number	41	0.442322
warehouse_size_string	41	0.442322
bytes_to_tb	41	0.442322
query_history	111	0.123355
try_to_date	13	0.090085
warehouse_load_history	170,274	0
bitor	8	0.008116
timestampadd	5	0.006608
cost_warehouse_size_translate	11	0.004058
cost_warehouse_size_untranslate	10	0.004058
dayname	10	0.004058
divide	1	0.004058
current_available_roles	12587	0.001916
all_user_names	1820	0.00081
is_trust_center_admin	11	0.00034
warehouse_metering_history	23	0
timestampdiff	14	0
current_role	9	0
tables	6	0
explain_json	6	0
copy_history	4	0
parse_warehouse_name	3	0
is_role_in_session	2	0
current_time	1	0
perform_right_size_warehouse	1	0
concat_with_dot	1	0
cost_warehouse	1	0
json_extract_path_text	1	0"""

# COMMAND ----------

profiler_functions = []
for line in raw_table.split("\n"):
    parts = line.split("\t")
    profiler_functions.append({"function_name": parts[0], "RUN_COUNT": parts[1], "TOTAL_CREDITS": parts[2]})


display(profiler_functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scrape Docs for Functions

# COMMAND ----------

# MAGIC %pip install requests beautifulsoup4

# COMMAND ----------

import requests
from bs4 import BeautifulSoup

def scrape_databricks_functions(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, "html.parser")
        # Scope selection to the specific section
        section = soup.select_one("#alphabetical-list-of-built-in-functions")
        if not section:
            return {"error": "Section not found"}
        
        items = section.select("ul li a")
        
        return [
            {
                "function_name": item.text.strip().split(" ")[0],
                "href": item["href"]
            }
            for item in items if item.has_attr("href")
        ]
    except Exception as e:
        return {"error": str(e)}

# Example usage
# url = "https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin-alpha.html"
# functions_list = scrape_databricks_functions(url)
# print(functions_list)


# Example usage
# url = "https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin-alpha.html"
# functions_list = scrape_databricks_functions(url)
# print(functions_list)

# Example usage
url = "https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin-alpha.html"
sql_functions = scrape_databricks_functions(url)

# COMMAND ----------

display(sql_functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare the lists

# COMMAND ----------

def compare_function_lists(list1, list2):
    set1 = {d['function_name'] for d in list1}
    set2 = {d['function_name'] for d in list2}
    
    common_functions = set1.intersection(set2)
    unique_to_list1 = set1 - set2
    unique_to_list2 = set2 - set1
    
    return {
        "common_functions": list(common_functions),
        "unique_to_list1": list(unique_to_list1),
        "unique_to_list2": list(unique_to_list2)
    }

results = compare_function_lists(profiler_functions, sql_functions)


# COMMAND ----------

from pprint import pprint as pp
# print(results)
pp(results)
