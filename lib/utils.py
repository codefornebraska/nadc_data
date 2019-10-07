import csvkit, os, traceback
import pandas as pd
from canonical.canonical import *

class utils(object):
    """ Utility class to provide helper functions for parsing """

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)


    def canonFlag(self, input):
        """
        Temporary workaround to display front-page canonical records
        """
        try:
            x = CANON[input]
            return "I"
        except:
            return ""


    def canonOffice(self,rawstring, param):
        """
        Hit the office canonical dict to standardize office names
        """
        try:
            x = CANON_OFFICE[rawstring][param]
            return x
        except:
            return ""


    def dedupeEntity(self,data, idList):
        """
        Dedupe entity file
        =========
        - csvsort entity_raw.txt by date_we_care_about
        - loop over unique entity IDs (having taken the set of id_master_list)
        - grep for each ID in the sorted entity file (~1 million times faster than python)
        - loop over the results, compiling a dict with the most recent, non-empty values, if available
        - in the process, kill out variants of "(DISSOLVED)" and other garbage strings
        - punch that record into a list
        - write that list to file
        - make one more pass to handle the handful of remaining entities that don't have a name
        """
    
        print("\n\nPREPPING ENTITY FILE")
    
        #get list of unique entity IDs
        uniques = list(set(idList))
        
        print("   pre-duping ...")
    
        #dedupe sorted file
        clean_entity = pd.read_csv(data, delimiter="|", dtype={
            "nadcid": object,
            "name": object,
            "address": object,
            "city": object,
            "state": object,
            "zip": object,
            "entity_type": object,
            "notes": object,
            "employer": object,
            "occupation": object,
            "place_of_business": object,
            "dissolved_date": object,
            "date_we_care_about": object,
            }
        )
    
        deduped_entities = clean_entity.drop_duplicates(subset=["nadcid", "name", "address", "city", "state", "zip", "entity_type", "notes", "employer", "occupation", "place_of_business", "dissolved_date"])       
        print("   sorting ...")
        #sort input file by date and grep?
        deduped_entities = deduped_entities.sort_values(by=14,axis=1)
        deduped_entities = deduped_entities.replace(to_replace ='1d', value = '', regex = True)
        dataFrameForStrays = pd.DataFrame(columns = ['id', 'canonical_id', 'name', 'canon_name', 'address', 'city', 'state', 'zip', 'entity_type', 'notes', 'employer', 'occupation', 'place_of_business', 'dissolved_date'])

        #get most current, complete data
        print("   grepping pre-duped, sorted file and deduping for recency and completeness ...")
    
        for idx, i in enumerate(uniques):
            #print str(idx)           
            resultsDF = deduped_entities[deduped_entities['nadcid'].str.match(i)]
            interimdict = {}
                
            #set default values
            interimdict['id'] = ""
            interimdict['canonical_id'] = ""
            interimdict['name'] = ""
            interimdict['canon_name'] = ""
            interimdict['address'] = ""
            interimdict['city'] = ""
            interimdict['state'] = ""
            interimdict['zip'] = ""
            interimdict['entity_type'] = ""
            interimdict['notes'] = ""
            interimdict['employer'] = ""
            interimdict['occupation'] = ""
            interimdict['place_of_business'] = ""
            interimdict['dissolved_date'] = ""
                
            # Using itertuples instead of iterrows to preserve dtypes? -> https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
            for dataFrameRow in resultsDF.itertuples(name='Row'):
                tempRow = dataFrameRow
                if not pd.isna(tempRow.name):
                    nadcid = tempRow.nadcid
                    name = tempRow.name
                    canonical_id = self.lookItUp(nadcid, "canonicalid", name)
                    canonical_name = self.lookItUp(nadcid, "canonicalname", name)
                    
                    interimdict['id'] = nadcid
                    interimdict['canonical_id'] = canonical_id
                    
                    #Unpack lookup to replace known bad strings
                    for item in GARBAGE_STRINGS:
                        name = name.upper().replace(*item).strip().rstrip(",").rstrip(" -")
                        canonical_name = canonical_name.upper().replace(*item).strip().rstrip(",").rstrip(" -")
                    
                    #check for complete names
                    if len(name) > 1:
                        interimdict['name'] = name
                    if len(canonical_name) > 1:
                        interimdict['canon_name'] = canonical_name
                    
                    #check for complete address
                    if not pd.isna(tempRow.address) and not pd.isna(tempRow.city) and not pd.isna(tempRow.state) and not pd.isna(tempRow.zip):
                        interimdict['address'] = tempRow.address
                        interimdict['city'] = tempRow.city
                        interimdict['state'] = tempRow.state
                        interimdict['zip'] = tempRow.zip

                    #check for complete entity type
                    if not pd.isna(tempRow.entity_type):
                        interimdict['entity_type'] = tempRow.entity_type

                    #check for complete employer
                    if not pd.isna(tempRow.employer):
                        interimdict['employer'] = tempRow.employer
                        
                    #check for complete occupation
                    if not pd.isna(tempRow.occupation):
                        interimdict['occupation'] = tempRow.occupation
                        
                    #check for complete place of business
                    if not pd.isna(tempRow.place_of_business):
                        interimdict['place_of_business'] = tempRow.place_of_business
                    
                    #check for complete dissolved date
                    if not pd.isna(tempRow.dissolved_date):
                        interimdict['dissolved_date'] = tempRow.dissolved_date

                if not interimdict['name'] or interimdict['name'] == "":
                        interimdict['name'] = "Name missing"
                        interimdict['canonical_name'] = "Name missing"
                        interimdict['notes'] = "Identifying information for several dozen committees and other entities, including this one, have been \"lost in digital space,\" according to the NADC."

                #append dict items to list
                outlist = {
                    'id': interimdict['id'],
                    'canonical_id': ['canonical_id'],
                    'name': interimdict['name'],
                    'canon_name': interimdict['canon_name'],
                    'address': interimdict['address'],
                    'city': interimdict['city'],
                    'state': interimdict['state'],
                    'zip': interimdict['zip'],
                    'entity_type': interimdict['entity_type'],
                    'notes': "",
                    'employer': interimdict['employer'],
                    'occupation': interimdict['occupation'],
                    'place_of_business': interimdict['place_of_business'],
                    'dissolved_date': interimdict['dissolved_date']
                }

                dataFrameForStrays.append(outlist, ignore_index=True)

        return dataFrameForStrays
            

    def dedupeDonations(self, data):
        """
        Dedupe donations file
        =========
        - call pandas drop_duplicates on a subset of fields
        - csvcut the columns we need out of this one
        - chop the header row and kill stray quotes
        """

        print("\n\nPREPPING DONATIONS FILE")
        print("    deduping ...")

        resultsDF = pd.DataFrame(columns = ["db_id", "cash", "inkind", "pledge", "inkind_desc", "donation_date", "donor_id", "recipient_id", "donation_year", "notes", "stance", "donor_name", "source_table"])
        clean_donations = pd.read_csv(data, delimiter="|", dtype={
            "db_id": object,
            "cash": object,
            "inkind": object,
            "pledge": object,
            "inkind_desc": object,
            "donation_date": object,
            "donor_id": object,
            "recipient_id": object,
            "donation_year": object,
            "notes": object,
            "stance": object,
            "donor_name": object,
            "source_table": object
            }
        )
        deduped_donations = clean_donations.drop_duplicates(subset=["donor_id", "donation_year", "donation_date", "recipient_id", "cash", "inkind", "pledge"])
        # Set up new dataframe with new columns, drop empties, sort, and regex replace 1d to ''
        deduped_donations = deduped_donations[['db_id,cash', 'inkind,pledge', 'inkind_desc', 'donation_date', 'donor_id', 'recipient_id', 'donation_year,notes', 'stance', 'donor_name', 'source_table']]
        deduped_donations.dropna()
        deduped_donations.dropna(axis='columns')
        deduped_donations = deduped_donations.sort_values(by=14,axis=1)
        deduped_donations = deduped_donations.replace(to_replace ='1d', value = '', regex = True)

        for dataFrameRow in deduped_donations.itertuples(name='Row'):
            tempRow = dataFrameRow
            db_id = tempRow.db_id
            cash = tempRow.cash
            inkind = tempRow.inkind
            pledge = tempRow.pledge
            inkind_desc = tempRow.inkind_desc
            donation_date = tempRow.donation_date
            donor_id = tempRow.donor_id
            recipient_id = tempRow.recipient_id
            donation_year = tempRow.donation_year
            notes = tempRow.notes
            week = tempRow.week
            donor_name = tempRow.donor_name
            source_table = tempRow.source_table

            if int(float(cash)) > 0 and int(float(inkind)) > 0:
                ls1 = [db_id, cash, "0.0", pledge, inkind_desc, donation_date, donor_id, recipient_id, donation_year, notes, week, donor_name, source_table]
                ls2 = [db_id, "0.0", inkind, pledge, inkind_desc, donation_date, donor_id, recipient_id, donation_year, notes, week, donor_name, source_table]
                resultsDF.append(ls1)
                resultsDF.append(ls2)
            else:
                resultsDF.append(tempRow)

        return resultsDF


    def getDate():
        # Parse the "last updated" date from a file in the NADC data dump
        q = open(THISPATH + "nadc_data/last_updated.py", "wb")
        with open(THISPATH + "nadc_data/DATE_UPDATED.TXT", "rb") as d:
            last_updated = d.readline().split(": ")[1].split(" ")[0].split("-")
            year = last_updated[0]
            month = last_updated[1].lstrip("0")
            day = last_updated[2].lstrip("0")
            q.write("import datetime\n\nLAST_UPDATED = datetime.date(" + year + ", " + month + ", " + day + ")")
        q.close()


    def getFloat(self, i):
        # Return a float or 0.0
        if not i or i == "":
            return "0.0"
        else:
            return str(float(i))


    def handleExpenditures(self, dataIn, _delimiter):  
        try:
            output = None
            reader = csvkit.reader(dataIn, delimiter=_delimiter)
            print(dataIn)
            for row in reader:
                db_id = row[0]
                payee = row[1]
                payee_addr = row[2]
                exp_date = row[3]
                exp_purpose = row[4]
                amount = row[5]
                inkind = row[6]
                committee_id = row[7]
                stance = row[8]
                notes = row[9]
                payee_committee_id = row[10]
                committee_exp_name = row[11]
                raw_target = row[12]
                target_candidate_id = row[13]
                target_committee_id = row[14]
            
                if int(float(amount)) > 0 and int(float(inkind)) > 0:
                    ls1 = [db_id, payee, payee_addr, exp_date, exp_purpose, amount, "0.0", committee_id, stance, notes, payee_committee_id, committee_exp_name, raw_target, target_candidate_id, target_committee_id]

                    ls2 = [db_id, payee, payee_addr, exp_date, exp_purpose, "0.0", inkind, committee_id, stance, notes, payee_committee_id, committee_exp_name, raw_target, target_candidate_id, target_committee_id]
                
                    output = ("|".join(ls1) + "\n")
                    output += ("|".join(ls2) + "\n")
                else:
                    output = ("|".join(row) + "\n")
            return output
        except:
            output = None
            showError = input("\n\nExpenditures are either empty, or there was an error. Would you like to see the error? ( Y = yes, N = No) : ")
            showError = showError.upper()
            if(showError == "Y"):
                traceback.print_exc()
            return output


    def hasBadDates(self, dateToBeChecked):
        if len(dateToBeChecked) > 0:
            if len(dateToBeChecked) == 1:
                s = "1 record with a bad date"
            else:
                s = str(len(dateToBeChecked)) + " records with bad dates"
            print("\n\nFound " + s + ". Go fix in canonical.py:")
         

    def lookItUp(self, input, param, namefield):
        # Check if a record exists in canonical donors lookup dict
        try:
            return str(CANON[input][param])
        except:
            if param == "canonicalid":
                return input
            else:
                return namefield


    def openFile(self,fileToBeOpened, parsingMethod):
        try:
            # Export is the boolean value recevied from the parsing method telling us if we failed or not
            with open(os.getcwd() + fileToBeOpened, 'r') as openFile:
                export = parsingMethod(openFile)
            return export
        except Exception as err:
            print('Open File Error!')
            traceback.print_exc()
            return False


    def validDate(self, datestring):
    # Check for known bad dates, invalid dates, dates in the future
        try:
            return GARBAGE_DATES[datestring]
        except:
            try:
                # is it a valid date?
                x = datetime.datetime.strptime(datestring, '%Y-%m-%d')
                # is the date before today?
                if x < datetime.datetime.now():
                    return datestring
                else:
                    return "broke"
            except:
                return "broke"