import logging
import json
import requests
import datetime
import pytz
import azure.functions as func
import azure.storage.table
from azure.storage.table import TableService, Entity


## Util function to evaluate date difference
def daysBetween(d1, d2):
    d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


## Util function to get a date daysNumber days
# before today
def daysAgo(daysNumber):
    today = datetime.date.today()
    lastWeek = today - datetime.timedelta(days = daysNumber)
    startingDate = lastWeek.strftime("%Y-%m-%d") + " 00:00:00"
    return startingDate


## Function to query Azure db using requested device name
def getDeviceSummary(tableName, tableService, devName):
    response = {}
    jsonResponse = {}
    samples = []
    dailySample = {}

    startingDate = datetime.datetime.strptime(daysAgo(16), "%Y-%m-%d %H:%M:%S")
    startingDate = str(int(startingDate.replace(tzinfo=pytz.utc).timestamp()))

    requestQuery = "PartitionKey eq 'AirSample' and DeviceName eq '" + devName +  "' and RowKey ge '" + startingDate + "'"

    try:
        # Query db
        entities = tableService.query_entities(tableName, filter = requestQuery)
        for entity in entities:
            sample = json.loads(entity.SampleValues)
            dailySample = {
                "time"      : sample["day"],
                "avgTemp"   : sample["avgDailyTemp"],
                "avgCo2"    : sample["avgDailyCo2"],
                "avgRad"    : sample["avgDailyRad"],
                "avgO3"     : sample["avgDailyO3"],
                "avgNo2"    : sample["avgDailyNo2"],
                "avgCo"     : sample["avgDailyCo"],
                "avgVoc"    : sample["avgDailyVoc"],
                "avgPm2_5"  : sample["avgDailyPm2_5"],
                "avgPm10"   : sample["avgDailyPm10"],
                "avgDs18"   : sample["avgDailyDs18"]
            }
            samples.append(dailySample.copy())

        # Build json responses
        response = { "samples" : samples }
        jsonResponse = json.dumps(response) 

    except Exception as error:
        logging.info(error)

    return jsonResponse


## Main function
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    accountName = ""
    accountKey = ""

    # Get params
    deviceName = req.params.get('device-name')
    if not deviceName or deviceName == "" or not deviceName.isalnum():
        return func.HttpResponse(
                "Unable to parse requested device name",
                status_code = 400)

    # Instantiate db connection
    tableName = 'AirSamples'
    tableService = None
    try:
        tableService = TableService(account_name=accountName, account_key=accountKey)  
    except Exception as error:
        logging.info(error)
        return func.HttpResponse(
                "Unable to connect to Azure Table",
                status_code=500)

    logging.info("Requested summary data for device " + deviceName)

    # Return response
    jsonResponse = getDeviceSummary(tableName, tableService, deviceName)
    return func.HttpResponse(jsonResponse, mimetype="application/json")