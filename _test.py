from lib import data_layer
import json
# parse form b6 somehow

config = None
with open('config.json', 'r') as configJSON:
            config = json.load(configJSON)


data = data_layer.data_layer()
print(data.fetchData(config[0]['dataURL']))
