import logging
import json
import requests
import datetime
import pytz
import azure.functions as func
import azure.storage.table
from azure.storage.table import TableService, Entity

## Function to query the Azure db with requested params
def getDeviceInfo(tableName, tableService, devName, startTime, endTime):
    response = {}
    jsonResponse = {}
    samples = []
    dailySample = {}
    reduceDataSet = False
    samplesThreshold = 20
    entitiesCount = 0

    requestedDays = daysBetween(startTime, endTime)

    # Adding time to requested days in order to correctly query the dbs
    startTime = datetime.datetime.strptime(startTime + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    endTime = datetime.datetime.strptime(endTime + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    startTime = str(int(startTime.replace(tzinfo=pytz.utc).timestamp()))
    endTime = str(int(endTime.replace(tzinfo=pytz.utc).timestamp()))

    requestQuery = "PartitionKey eq 'AirSample' and DeviceName eq '" + devName +  "' and RowKey ge '" + startTime + "' and RowKey le '" + endTime + "'"
    try:
        entities = tableService.query_entities(tableName, filter = requestQuery)
        entitiesCount = sum(1 for x in entities)
        
        # If the dataset need to be shorter (a lot of data has been requested)
        if requestedDays >= samplesThreshold and entitiesCount >= samplesThreshold:
            reduceDataSet = True

        for entity in entities:
            sample = json.loads(entity.SampleValues)
            if reduceDataSet:
                dailySample = {
                    "time"          : sample["day"],
                    "missingData"   : "false",
                    "avgTemp"       : sample["avgDailyTemp"],
                    "avgCo2"        : sample["avgDailyCo2"],
                    "avgRad"        : sample["avgDailyRad"],
                    "avgO3"         : sample["avgDailyO3"],
                    "avgNo2"        : sample["avgDailyNo2"],
                    "avgCo"         : sample["avgDailyCo"],
                    "avgVoc"        : sample["avgDailyVoc"],
                    "avgPm2_5"      : sample["avgDailyPm2_5"],
                    "avgPm10"       : sample["avgDailyPm10"],
                    "avgDs18"       : sample["avgDailyDs18"]
                }
                samples.append(dailySample.copy())
            else:
                samples += sample["data"]

        # Build the response and converting it to jsons
        response = { "samples" : samples }
        jsonResponse = json.dumps(response) 

    except Exception as error:
        logging.info(error)

    return jsonResponse


## Util function to evaluate date difference
def daysBetween(d1, d2):
    d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


## Util function to evaluate if requested date
# format is corrects
def dateValidation(dateStr):
    dateFormat = "%Y-%m-%d"

    try:
      datetime.datetime.strptime(dateStr, dateFormat)
      return True
    except ValueError:
      return False


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
                status_code = 400
            )

    startTime = req.params.get('from')
    if not startTime or startTime == "":
        return func.HttpResponse(
                "Unable to parse requested start date",
                status_code = 400
            )

    endTime = req.params.get('to')
    if not endTime or endTime == "":
        return func.HttpResponse(
                "Unable to parse requested end date",
                status_code = 400
            )

    # Date validation
    if not dateValidation(startTime):
        return func.HttpResponse(
                "Unable to parse start date format: should be YYYY-mm-dd",
                status_code = 400
            )

    if not dateValidation(endTime):
        return func.HttpResponse(
                "Unable to parse end date format: should be YYYY-mm-dd",
                status_code = 400
            )

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

    logging.info("Requested device " + deviceName + " data for data range " + startTime + " - " + endTime)

    # Return response
    jsonResponse = getDeviceInfo(tableName, tableService, deviceName, startTime, endTime)
    return func.HttpResponse(jsonResponse, mimetype="application/json")