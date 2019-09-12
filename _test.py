from lib import data_layer, parser
import json, os
# parse form b6 somehow
# finish dedupe functions in utils

config = None
with open('config.json', 'r') as configJSON:
            config = json.load(configJSON)


newData = True
data = data_layer.data_layer()
parse = parser.parser('FirstInstance')

# check last updated to see if we need to run
if(False):
    newData = data.fetchData(config[0]['dataURL'])

if(newData):
    with open(os.getcwd() + '\\downloaded-data\\nadc_data\\forma1.txt', 'r') as FormA1:
        parse.parseForma1(FormA1)

parse.destroy()

