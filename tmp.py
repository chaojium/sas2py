from databricks import sql
import os 
from dotenv import load_dotenv
load_dotenv()
conn = sql.connect(
    server_hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"],
    http_path = os.environ["DATABRICKS_HTTP_PATH"],
    access_token = os.environ["DATABRICKS_ACCESS_TOKEN"]
)

cursor = conn.cursor()
cursor.execute("SELECT 1")
result = cursor.fetchall()

print(result)

cursor.close()
conn.close()