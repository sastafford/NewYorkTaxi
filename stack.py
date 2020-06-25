import configparser
import json


def getProperties(propertiesName):
    properties = configparser.ConfigParser()
    properties.read(propertiesName)
    return properties


def getConfig(configurationName):
    with open('./config.json') as f:
        config = json.load(f)
    return config


def replaceWorkspaceHome(config, properties):
    for resource in config.get("resources"):
        workspaceHome = properties["DATABRICKS"]["WORKSPACE_HOME"]
        path = resource.get("properties").get("path")
        newWorkspacePath = path.replace("%WORKSPACE_HOME%", workspaceHome)
        resource["properties"]["path"] = newWorkspacePath
        return config


def writeNewConfig(config):
    with open('config-new.json', 'w') as new_config:
        json.dump(config, new_config, indent=4)


config = getConfig("./config.json")
properties = getProperties("stack.properties")
replaceWorkspaceHome(config, properties)
writeNewConfig(config)
