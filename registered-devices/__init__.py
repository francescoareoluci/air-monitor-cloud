import logging
import json
import requests
import azure.functions as func
import azure.storage.table
from azure.storage.table import TableService, Entity


## Function to query Azure db to get all
# configured devices
def getStoredDevices(tableName, tableService):
    response = {}
    jsonResponse = {}
    devices = []
    deviceEntry = {}

    try:
        # Query db
        entities = tableService.query_entities(tableName, filter = "PartitionKey eq 'Device'")

        for entity in entities:
            print(entity)
            deviceEntry = {
                "deviceName" : entity.DeviceName,
                "latitude"   : entity.Latitude,
                "longitude"  : entity.Longitude
            }
            devices.append(deviceEntry)

        logging.info(devices)

        # Build json response
        response = { "devices" : devices }
        jsonResponse = json.dumps(response) 

    except Exception as error:
        logging.info(error)

    return jsonResponse


## Main function
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    accountName = ""
    accountKey = ""

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

    logging.info('Requested registered devices')

    # Return response
    jsonResponse = getStoredDevices(tableName, tableService)
    return func.HttpResponse(jsonResponse, mimetype="application/json")