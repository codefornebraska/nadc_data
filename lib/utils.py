class utils(object):
    """ Utility class to provide helper functions for parsing """

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def validDate(datestring):
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

        
    def getFloat(i):
        # Return a float or 0.0
    
        if not i or i == "":
            return "0.0"
        else:
            return str(float(i))


    def lookItUp(input, param, namefield):
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


    def canonFlag(input):
        """
        Temporary workaround to display front-page canonical records
        """
    
        try:
            x = CANON[input]
            return "I"
        except:
            return ""


    def canonOffice(rawstring, param):
        """
        Hit the office canonical dict to standardize office names
        """
    
        try:
            x = CANON_OFFICE[rawstring][param]
            return x
        except:
            return ""