import configparser
import json

config = configparser.ConfigParser()
config.read('stack.properties')
workspace_home = config["DATABRICKS"]["WORKSPACE_HOME"]

with open('./config.json') as f:
    config_json = json.load(f)

for resource in config_json.get("resources"):
    path = resource.get("properties").get("path")
    new_workspace_path = path.replace("%WORKSPACE_HOME%", workspace_home)
    resource["properties"]["path"] = new_workspace_path

with open('config-new.json', 'w') as new_config:
    json.dump(config_json, new_config, indent=4)
