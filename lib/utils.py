import csvkit, os, traceback

class utils(object):
    """ Utility class to provide helper functions for parsing """

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

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

        
    def getFloat(self, i):
        # Return a float or 0.0
    
        if not i or i == "":
            return "0.0"
        else:
            return str(float(i))


    def lookItUp(self, input, param, namefield):
        # Check if a record exists in canonical donors lookup dict
    
        try:
            return str(CANON[input][param])
        except:
            if param == "canonicalid":
                return input
            else:
                return namefield

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

    def isBadDate(self, dateToBeChecked):
        if len(dateToBeChecked) > 0:
            if len(dateToBeChecked) == 1:
                s = "1 record with a bad date"
            else:
                s = str(len(dateToBeChecked)) + " records with bad dates"
            print("\n\nFound " + s + ". Go fix in canonical.py:")
            for date in dateToBeChecked:
                print(date)

    def handleExpenditures(self, dataIn, _delimiter):
        output = None
        reader = csvkit.reader(dataIn, delimiter=_delimiter)
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
                return output
            else:
                output = ("|".join(row) + "\n")
                return output

    def openFile(self,fileToBeOpened, parsingMethod):
        try:
            # Export is the boolean value recevied from the parsing method telling us if we failed or not
            with open(os.getcwd() + fileToBeOpened, 'r') as openFile:
                export = parsingMethod(openFile)
            return export
        except:
            traceback.print_exc()
            return False



    def dedupeEntity(data, idList):

        """

        ===================================================================================================================
        =                                                                                                                 =
        =                      DEDUPING NOT DONE!!                                                                        =
        =                                                                                                                 =
        ===================================================================================================================



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
    
        deduped_entities.to_csv(os.getcwd() + '\temp\entities_deduped.txt', sep="|")
    
        print("   sorting ...")

        #sort input file by date
        with hide('running', 'stdout', 'stderr'):
            local('csvsort -d "|" -c 14 ' + THISPATH + 'nadc_data/toupload/entities_deduped.txt | csvformat -D "|" | sed -e \'1d\' > ' + THISPATH + 'nadc_data/toupload/entities_sorted_and_deduped.txt', capture=False)
    
        #get most current, complete data
        print("   grepping pre-duped, sorted file and deduping for recency and completeness ...")
    
        with open(THISPATH + "nadc_data/toupload/entity_almost_final_for_real.txt", "wb") as entity_almost_final:
            for idx, i in enumerate(uniques):
                #print str(idx)
                with hide('running', 'stdout', 'stderr'):
                    grepstring = local('grep "' + i + '" ' + THISPATH + 'nadc_data/toupload/entities_sorted_and_deduped.txt', capture=True)
                    g = grepstring.split("\n") #list of records that match
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
                    interimdict['employer'] = ""
                    interimdict['occupation'] = ""
                    interimdict['place_of_business'] = ""
                    interimdict['dissolved_date'] = ""
                
                    for dude in g:
                        row = dude.split("|") #actual record
                    
                        nadcid = row[1]
                        name = row[2]
                        canonical_id = lookItUp(nadcid, "canonicalid", name)
                        canonical_name = lookItUp(nadcid, "canonicalname", name)
                    
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
                        if len(row[3]) > 1 and len(row[4]) > 1 and len(row[5]) > 1 and len(row[6]) > 1:
                            interimdict['address'] = row[3]
                            interimdict['city'] = row[4]
                            interimdict['state'] = row[5]
                            interimdict['zip'] = row[6]

                        #check for complete entity type
                        if len(row[7]) >= 1:
                            interimdict['entity_type'] = row[7]

                        #check for complete employer
                        if len(row[9]) > 1:
                            interimdict['employer'] = row[9]
                        
                        #check for complete occupation
                        if len(row[10]) > 1:
                            interimdict['occupation'] = row[10]
                        
                        #check for complete place of business
                        if len(row[11]) > 1:
                            interimdict['place_of_business'] = row[11]
                    
                        #check for complete dissolved date
                        if len(row[12]) > 1:
                            interimdict['dissolved_date'] = row[12]

                    #append dict items to list
                    outlist = [
                        interimdict['id'],
                        interimdict['canonical_id'],
                        interimdict['name'],
                        interimdict['canon_name'],
                        interimdict['address'],
                        interimdict['city'],
                        interimdict['state'],
                        interimdict['zip'],
                        interimdict['entity_type'],
                        "",
                        interimdict['employer'],
                        interimdict['occupation'],
                        interimdict['place_of_business'],
                        interimdict['dissolved_date']
                    ]
                
                    entity_almost_final.write("|".join(outlist) + "\n")
                
        #handling stray bastards with no names
        print("   handling entities with no name ...")
    
        with open(THISPATH + "nadc_data/toupload/entity_almost_final_for_real.txt", "rb") as readin, open(THISPATH + "nadc_data/toupload/entity.txt", "wb") as readout:
            reader = csvkit.reader(readin, delimiter=delim)
            for row in reader:
                nadc_id = row[0]
                canonical_id = row[1]
                name = row[2]
                canonical_name = row[3]
                address = row[4]
                city = row[5]
                state = row[6]
                zip = row[7]
                entity_type = row[8]
                notes = row[9]
                employer = row[10]
                occupation = row[11]
                biz = row[12]
                dissolved_date = row[13]
                if not name or name == "":
                    name = "Name missing"
                    canonical_name = "Name missing"
                    notes = "Identifying information for several dozen committees and other entities, including this one, have been \"lost in digital space,\" according to the NADC."
            
                outlist = [nadc_id, canonical_id, name, canonical_name, address, city, state, zip, entity_type, notes, employer, occupation, biz, dissolved_date, ""]
            
                readout.write("|".join(outlist) + "\n") 
                
    def dedupeDonations(data, idList):
        """

        ===================================================================================================================
        =                                                                                                                 =
        =                      DEDUPING NOT DONE!!                                                                        =
        =                                                                                                                 =
        ===================================================================================================================


        Dedupe donations file
        =========
        - call pandas drop_duplicates on a subset of fields
        - csvcut the columns we need out of this one
        - chop the header row and kill stray quotes
        """

        print("\n\nPREPPING DONATIONS FILE")
        print("    deduping ...")
    
        clean_donations = pd.read_csv(THISPATH + "nadc_data/toupload/donations_raw.txt", delimiter="|", dtype={
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
        deduped_donations.to_csv(THISPATH + 'nadc_data/toupload/donations_almost_there.txt', sep="|")
    
        with hide('running', 'stdout', 'stderr'):
            local('csvcut -x -d "|" -c db_id,cash,inkind,pledge,inkind_desc,donation_date,donor_id,recipient_id,donation_year,notes,stance,donor_name,source_table ' + THISPATH + 'nadc_data/toupload/donations_almost_there.txt | csvformat -D "|" | sed -e \'1d\' -e \'s/\"//g\' > ' + THISPATH + 'nadc_data/toupload/donations_almost_there_for_real.txt', capture=False)
    
        with open(THISPATH + 'nadc_data/toupload/donations_almost_there_for_real.txt', 'rb') as don_in, open(THISPATH + 'nadc_data/toupload/donations.txt', 'wb') as don_out:
            reader = csvkit.reader(don_in, delimiter=delim)
            for don_record in reader:
                db_id = don_record[0]
                cash = don_record[1]
                inkind = don_record[2]
                pledge = don_record[3]
                inkind_desc = don_record[4]
                donation_date = don_record[5]
                donor_id = don_record[6]
                recipient_id = don_record[7]
                donation_year = don_record[8]
                notes = don_record[9]
                week = don_record[10]
                donor_name = don_record[11]
                source_table = don_record[12]
                if int(float(cash)) > 0 and int(float(inkind)) > 0:
                    ls1 = [db_id, cash, "0.0", pledge, inkind_desc, donation_date, donor_id, recipient_id, donation_year, notes, week, donor_name, source_table]
                    ls2 = [db_id, "0.0", inkind, pledge, inkind_desc, donation_date, donor_id, recipient_id, donation_year, notes, week, donor_name, source_table]
                    don_out.write("|".join(ls1) + "\n")
                    don_out.write("|".join(ls2) + "\n")
                else:
                    don_out.write("|".join(don_record) + "\n")
    
        print("\n\nDONE.")


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