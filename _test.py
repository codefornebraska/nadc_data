
from lib import data_layer, parser, utils
import json, os
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
    parsedFormA1 = utility.openFile('\\downloaded-data\\nadc_data\\forma1.txt', parse.parseForma1)
    if(parsedFormA1):
        parsedFormA1Cand = utility.openFile('\\downloaded-data\\nadc_data\\forma1cand.txt',parse.parseForma1Cand)
        if(parsedFormA1Cand):
            parsedFormB1 = utility.openFile('\\downloaded-data\\nadc_data\\formb1.txt', parse.parseFormB1)
            if(parsedFormB1):
                parsedFormB1AB = utility.openFile('\\downloaded-data\\nadc_data\\formb1ab.txt', parse.parseFormB1AB)
                if(parsedFormB1AB):
                    parsedFormB1C = utility.openFile('\\downloaded-data\\nadc_data\\formb1c.txt', parse.parseFormB1C)
                    if(parsedFormB1C):
                        parsedFormB1D = utility.openFile('\\downloaded-data\\nadc_data\\formb1d.txt', parse.parseFormB1D)
                        if(parsedFormB1D):
                            parsedFormB2 = utility.openFile('\\downloaded-data\\nadc_data\\formb2.txt', parse.parseFormB2)
                            if(parsedFormB2):
                                parsedFormB2A = utility.openFile('\\downloaded-data\\nadc_data\\formb2a.txt', parse.parseFormB2A)
                                if(parsedFormB2A):
                                    parsedFormB2B = utility.openFile('\\downloaded-data\\nadc_data\\formb2b.txt', parse.parseFormB2B)
                                    if(parsedFormB2B):
                                        parsedFormB4 = utility.openFile('\\downloaded-data\\nadc_data\\formb4.txt', parse.parseFormB4)
                                        if(parsedFormB4):
                                            parsedFormB4A = utility.openFile('\\downloaded-data\\nadc_data\\formb4a.txt', parse.parseFormB4A)
                                            if(parsedFormB4A):
                                                parsedFormB4B1 = utility.openFile('\\downloaded-data\\nadc_data\\formb4b1.txt', parse.parseFormB4B1)
                                                if(parsedFormB4B1):
                                                    parsedFormB4B2 = utility.openFile('\\downloaded-data\\nadc_data\\formb4b2.txt', parse.parseFormB4B2)
                                                    if(parsedFormB4B2):
                                                        parsedFormB4B3 = utility.openFile('\\downloaded-data\\nadc_data\\formb4b3.txt', parse.parseFormB4B3)
                                                        if(parsedFormB4B3):
                                                            parsedFormB5 = utility.openFile('\\downloaded-data\\nadc_data\\formb5.txt', parse.parseFormB5)
                                                            if(parsedFormB5):
                                                                # File Form must be B6 then B6 Expenditures
                                                                parsedFormB6 = parse.parseFormB6(os.getcwd() + '\\downloaded-data\\nadc_data\\formb6.txt', os.getcwd() + '\\downloaded-data\\nadc_data\\formb6expend.txt')
                                                                if(parsedFormB6):
                                                                    parsedFormB7 = utility.openFile('\\downloaded-data\\nadc_data\\formb7.txt', parse.parseFormB7)
                                                                    if(parsedFormB7):
                                                                        parsedFormB72 = utility.openFile('\\downloaded-data\\nadc_data\\formb72.txt', parse.parseFormB72)
                                                                        if(parsedFormB72):
                                                                            parsedFormB73 = utility.openFile('\\downloaded-data\\nadc_data\\formb73.txt', parse.parseFormB73)
                                                                            if(parsedFormB73):
                                                                                parsedFormB9 = utility.openFile('\\downloaded-data\\nadc_data\\formb9.txt', parse.parseFormB9)
                                                                                if(parsedFormB9):
                                                                                    parsedFormMisc = utility.openFile('\\downloaded-data\\nadc_data\\forma1misc.txt', parse.parseFormA1Misc)
                                                                                    if(parsedFormMisc):
                                                                                        badDates = parse.getRowsWithBadDates()
                                                                                        delim = parse.getDelimiter()
                                                                                        utility.hasBadDates(badDates)
                                                                                        expend = utility.handleExpenditures(os.getcwd() + '\\temp\\expenditure_raw.txt', delim)
                                                                                        if(expend is not None):
                                                                                            with open(os.getcwd + "\\temp\\expenditure.txt", "wb") as exp_out:
                                                                                                exp_out.write(bytes(expend, 'utf-8'))
                                                                                        dedupe = utility.dedupeEntity(os.getcwd() + '\\temp\\entity_raw.txt', parse.getIdMasterList())
    parse.destroy()

    if(parsedFormA1 and parsedFormA1Cand and parsedFormB1 and parsedFormB1AB and parsedFormB1C and parsedFormB1D and parsedFormB2 and parsedFormB2A and parsedFormB2B and parsedFormB4 and parsedFormB4A and parsedFormB4B1 and parsedFormB4B2 and parsedFormB4B3 and parsedFormB5 and parsedFormB6 and parsedFormB7 and parsedFormB72 and parsedFormB73 and parsedFormB9 and parsedFormMisc):
        print("\n\n.......Completed Successfully.")
    else:
        print("\n\n.......Parsing Failed.")

