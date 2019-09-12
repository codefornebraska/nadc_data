from lib import data_layer, parser, utils
import json, os
# parse form b6 somehow
# finish dedupe functions in utils

config = None
with open('config.json', 'r') as configJSON:
            config = json.load(configJSON)


newData = True
data = data_layer.data_layer()
utility = utils.utils()
parse = parser.parser('FirstInstance')

# check last updated to see if we need to run
if(False):
    newData = data.fetchData(config[0]['dataURL'])

if(newData):
    utility.openFile('\\downloaded-data\\nadc_data\\forma1.txt', parse.parseForma1)
    utility.openFile('\\downloaded-data\\nadc_data\\forma1cand.txt',parse.parseForma1Cand)
    utility.openFile('\\downloaded-data\\nadc_data\\formb1.txt', parse.parseFormB1)
    utility.openFile('\\downloaded-data\\nadc_data\\formb1ab.txt', parse.parseFormB1AB)
    utility.openFile('\\downloaded-data\\nadc_data\\formb1c.txt', parse.parseFormB1C)
    utility.openFile('\\downloaded-data\\nadc_data\\formb1d.txt', parse.parseFormB1D)
    utility.openFile('\\downloaded-data\\nadc_data\\formb2.txt', parse.parseFormB2)
    utility.openFile('\\downloaded-data\\nadc_data\\formb2a.txt', parse.parseFormB2A)
    utility.openFile('\\downloaded-data\\nadc_data\\formb2b.txt', parse.parseFormB2B)
    utility.openFile('\\downloaded-data\\nadc_data\\formb4.txt', parse.parseFormB4)
    utility.openFile('\\downloaded-data\\nadc_data\\formb4a.txt', parse.parseFormB4A)
    utility.openFile('\\downloaded-data\\nadc_data\\formb4b1.txt', parse.parseFormB4B1)
    utility.openFile('\\downloaded-data\\nadc_data\\formb4b2.txt', parse.parseFormB4B2)
    utility.openFile('\\downloaded-data\\nadc_data\\formb4b3.txt', parse.parseFormB4B3)
    utility.openFile('\\downloaded-data\\nadc_data\\formb5.txt', parse.parseFormB5)
    # Parse Form B6 Somehow
    utility.openFile('\\downloaded-data\\nadc_data\\formb7.txt', parse.parseFormB7)
    utility.openFile('\\downloaded-data\\nadc_data\\formb72.txt', parse.parseFormB72)
    utility.openFile('\\downloaded-data\\nadc_data\\formb73.txt', parse.parseFormB73)
    utility.openFile('\\downloaded-data\\nadc_data\\formb9.txt', parse.parseFormB9)
    utility.openFile('\\downloaded-data\\nadc_data\\forma1misc.txt', parse.parseFormA1Misc)

    parse.destroy()

