from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_ACCESS_TOKEN")
# Alternatively in a Databricks notebook you can use this:
# DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

if not DATABRICKS_TOKEN:
    raise RuntimeError("Set DATABRICKS_ACCESS_TOKEN before running this script.")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://adb-1881246389460182.2.azuredatabricks.net/serving-endpoints"
)

response = client.chat.completions.create(
    model="databricks-gemma-3-12b",
    messages=[
        {
            "role": "user",
            "content": "What is an LLM agent?"
        }
    ],
    max_tokens=5000
)

print(response.choices[0].message.content)
