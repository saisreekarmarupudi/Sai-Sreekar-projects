# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook generates JSON events for network data
# MAGIC ## The data is generated in the JSON format
# MAGIC ## We are using Faker library to randomly generate values for various JSON elements
# MAGIC ## We are storing the events as JSON in path like this dbfs_path = f"/mnt/volumes/network_data/event_{timestamp_str}.json"
# MAGIC ## Each event is stored as a file with name event_{dynamically generated timestamp}.json

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import json
from random import randint, choice
from faker import Faker

import json
from datetime import datetime

# Initialize Faker
fake = Faker()

def generate_random_event():
    # Generate random values for each field
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    source_ip = fake.ipv4()
    destination_ip = fake.ipv4()
    source_port = randint(1024, 65535)
    destination_port = choice([80, 443, 8080])
    protocol = choice(["TCP", "UDP"])
    action = choice(["ALLOW", "BLOCK"])
    bytes_sent = randint(100, 10000)
    packets = randint(1, 100)
    duration = f"{randint(1, 60)}s"
    rule_name = f"Rule {randint(1, 100)}"
    rule_id = f"RULE-{randint(100, 999)}"
    policy_name = choice(["Standard Access", "Restricted Access"])
    firewall_name = f"FW{randint(1, 3)}"
    direction = choice(["INBOUND", "OUTBOUND"])
    application = choice(["Web Browsing", "Email", "FTP", "SSH"])
    user = fake.user_name()
    location = choice(["Office-1", "Office-2", "Remote"])
    threat_level = choice(["Low", "Medium", "High", "N/A"])
    threat_description = "N/A" if threat_level == "N/A" else "Suspicious Activity Detected"

    # Generate user details
    user_details = {
        "user_id": user,
        "name": fake.name(),
        "email": fake.email(),
        "role": choice(["Analyst", "Manager", "Engineer", "Sales"]),
        "department": choice(["IT", "Sales", "Engineering", "Marketing"])
    }

    # Combine all data into a single event
    event = {
        "timestamp": timestamp,
        "source_ip": source_ip,
        "destination_ip": destination_ip,
        "source_port": source_port,
        "destination_port": destination_port,
        "protocol": protocol,
        "action": action,
        "bytes": bytes_sent,
        "packets": packets,
        "duration": duration,
        "rule_name": rule_name,
        "rule_id": rule_id,
        "policy_name": policy_name,
        "firewall_name": firewall_name,
        "direction": direction,
        "application": application,
        "user": user,
        "location": location,
        "threat_level": threat_level,
        "threat_description": threat_description,
        "users": [user_details]  # Add the user details as an array
    }

    return event

# Generate a sample event
sample_event = generate_random_event()

# Print the sample event as a JSON string
print(json.dumps(sample_event, indent=2))




event_json = json.dumps(sample_event)

# Generate a timestamp string
from datetime import datetime

# Generate a timestamp string
timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Define the DBFS path and file name with timestamp
dbfs_path = f"/mnt/volumes/network_data/event_{timestamp_str}.json"

# Use dbutils.fs.put to save the JSON string to the specified DBFS location
dbutils.fs.put(dbfs_path, event_json, overwrite=False)


# COMMAND ----------

# MAGIC %fs ls "/mnt/volumes/network_data/"

# COMMAND ----------

#%fs ls

# COMMAND ----------

#%fs ls /mnt/volumes/network_data

# COMMAND ----------

#%fs mkdirs /mnt/volumes/network_data

# COMMAND ----------

#%fs mkdirs /mnt/volumes/network_data/silver_max_date

# COMMAND ----------

#%fs mkdirs /mnt/volumes/