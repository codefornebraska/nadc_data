import requests, io, zipfile, json, os

class data_layer(object):
    """Fetch and Push Data"""

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def fetchData(self, downloadURL):
        req = requests.get(downloadURL)
        if(req.ok):
            zip = zipfile.ZipFile(io.BytesIO(req.content))
            zip.extractall(os.getcwd() + '\downloaded-data')
            return True
        else:
            return False



