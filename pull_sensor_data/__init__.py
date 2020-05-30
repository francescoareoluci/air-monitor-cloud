import logging
import csv
import json
import requests
import datetime
import pytz
import azure.functions as func
import azure.storage.table
from __app__.pull_sensor_data.DataStructures import DailyAverage, StructuredDate
from azure.storage.table import TableService, Entity


## Util function to build the request URL
# given the device name and the date
def buildRequestUrl(deviceName, year, month, day):
    requestYear = year
    requestMonth = month
    requestDay = day

    url = "http://www.sensorwebhub.org:8080/swhrest/rest/download/get_geodata_csv?year="+ requestYear 
    url += "&month=" + requestMonth 
    url += "&day=" + requestDay
    url += "&year2=" + requestYear
    url += "&month2=" + requestMonth 
    url += "&day2=" + requestDay + "&station_id=" + deviceName + "&user_id=guest&pwd_id=guest&language=it"

    return url


## Function to build the JSON which will be
# put in Azure db
def buildJson(samplesDict, dailyAverages, date):
    # Build the json as average daily values and average for each hour
    jsonObjects = []
    jsonObject = {}

    # Keys (hour) are from the most recent to the most old
    # Let's reverse them...
    for key in reversed(list(samplesDict.keys())):
        dictValues = samplesDict[key]

        jsonObject = {
            'time': key,
            'missingData': dictValues[11],
            'avgTemp': dictValues[0],
            'avgCo2': dictValues[1],
            'avgRad': dictValues[2],
            'avg03': dictValues[3],
            'avgNo2': dictValues[4],
            'avgCo': dictValues[5],
            'avgVoc': dictValues[6],
            'avgPm2_5': dictValues[7],
            'avgPm10': dictValues[8],
            'avgDs18': dictValues[9],
            'samplxH': dictValues[10]
        }
        jsonObjects.append(jsonObject)

    completeJson = {
        'day': date,
        'latitude': dailyAverages.avgLatitude,
        'longitude': dailyAverages.avgLongitude,
        'avgDailyTemp': dailyAverages.avgDailyTemp,
        'avgDailyCo2': dailyAverages.avgDailyCo2,
        'avgDailyRad': dailyAverages.avgDailyRad,
        'avgDailyO3': dailyAverages.avgDailyO3,
        'avgDailyNo2': dailyAverages.avgDailyNo2,
        'avgDailyCo': dailyAverages.avgDailyCo,
        'avgDailyVoc': dailyAverages.avgDailyVoc,
        'avgDailyPm2_5': dailyAverages.avgDailyPm2_5,
        'avgDailyPm10': dailyAverages.avgDailyPm10,
        'avgDailyDs18': dailyAverages.avgDailyDs18,
        'data': jsonObjects
    }
    jsonResult = json.dumps(completeJson)

    return jsonResult


## Function to parse the resulting csv file from request url
def parseCsv(listCsv, samplesDict, dailyAverages, processedDate):
    isFirstRow = True
    lastHour = 0
    hourDiff = 0

    for row in listCsv:
        # Reading data
        longitude   = row[1]
        latitude    = row[2]
        date        = row[3]
        co2         = row[4]
        temperature = row[5]
        rad         = row[6]
        o3          = row[7]
        no2         = row[8]
        co          = row[9]
        voc         = row[10]
        pm2_5       = row[11]
        pm10        = row[12]
        ds18        = row[13]

        # Splitting date
        dateArray = date.split('-')
        processedDate.year = dateArray[0]
        processedDate.month = dateArray[1]
        processedDate.day = dateArray[2].split(' ')[0]
        processedDate.hour = dateArray[2].split(' ')[1].split(':')[0]

        # Evaluate the sum to eval average daily values
        dailyAverages.avgLongitude += float(longitude)
        dailyAverages.avgLatitude += float(latitude)
        dailyAverages.avgDailyTemp += float(temperature)
        dailyAverages.avgDailyCo2 += float(co2)
        dailyAverages.avgDailyRad += float(rad)
        dailyAverages.avgDailyO3 += float(o3)
        dailyAverages.avgDailyNo2 += float(no2)
        dailyAverages.avgDailyCo += float(co)
        dailyAverages.avgDailyVoc += float(voc)
        dailyAverages.avgDailyPm2_5 += float(pm2_5)
        dailyAverages.avgDailyPm10 += float(pm10)
        dailyAverages.avgDailyDs18 += float(ds18)

        newHour = int(processedDate.hour)
        if isFirstRow:
            lastHour = newHour
            isFirstRow = False

            if newHour != 23:
                # Fill data
                for h in range(23, newHour, -1):
                    mapKey = processedDate.year + '-' + processedDate.month + '-' + processedDate.day + '_' + str(h).zfill(2)
                    samplesDict[mapKey] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "true"]
                    fillData = True

        else:
            hourDiff = lastHour - newHour
            fillData = False

            # Fill missing data
            if hourDiff > 1:
                for h in range(lastHour - 1, newHour, -1):
                    mapKey = processedDate.year + '-' + processedDate.month + '-' + processedDate.day + '_' + str(h).zfill(2)
                    samplesDict[mapKey] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "true"]
                    fillData = True

            lastHour = newHour

            if fillData:
                continue


        # Build the key for dict as 'YYYY-mm-DD_hh'
        mapKey = processedDate.year + '-' + processedDate.month + '-' + processedDate.day + '_' + processedDate.hour

        # Build the dict to mantain the sum of all data per hour 
        # and the number of samples
        if mapKey in samplesDict:
            dictValues = samplesDict[mapKey]

            newTemp = float(dictValues[0]) + float(temperature)
            newCo2 = float(dictValues[1]) + float(co2)
            newRad = float(dictValues[2]) + float(rad)
            newO3 = float(dictValues[3]) + float(o3)
            newNo2 = float(dictValues[4]) + float(no2)
            newCo = float(dictValues[5]) + float(co)
            newVoc = float(dictValues[6]) + float(voc)
            newPm2_5 = float(dictValues[7]) + float(pm2_5)
            newPm10 = float(dictValues[8]) + float(pm10)
            newDs18 = float(dictValues[9]) + float(ds18)

            totalValues = int(dictValues[10]) + 1

            samplesDict[mapKey] = [newTemp, newCo2, newRad, newO3, newNo2, newCo, newVoc, newPm2_5, newPm10, newDs18, totalValues, "false"]
        else:
            samplesDict[mapKey] = [temperature, co2, rad, o3, no2, co, voc, pm2_5, pm10, ds18, 1, "false"]

    if lastHour != 0:
        # Fill data
        for h in range(lastHour - 1, -1, -1):
            mapKey = processedDate.year + '-' + processedDate.month + '-' + processedDate.day + '_' + str(h).zfill(2)
            samplesDict[mapKey] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "true"]
            fillData = True


## Function for evaluate mean values of the given dict
def averageSamples(samplesDict):
    for key in samplesDict:
        dictValues = samplesDict[key]

        if dictValues[11] == "true":
            samplesDict[key] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "true"]
            continue

        totalValues = int(dictValues[10])

        ## Round values
        avgTemp = float("{:.2f}".format(float(dictValues[0]) / totalValues))
        avgCo2 = float("{:.2f}".format(float(dictValues[1]) / totalValues))
        avgRad = float("{:.2f}".format(float(dictValues[2]) / totalValues))
        avgO3 = float("{:.2f}".format(float(dictValues[3]) / totalValues))
        avgNo2 = float("{:.2f}".format(float(dictValues[4]) / totalValues))
        avgCo = float("{:.2f}".format(float(dictValues[5]) / totalValues))
        avgVoc = float("{:.2f}".format(float(dictValues[6]) / totalValues))
        avgPm2_5 = float("{:.2f}".format(float(dictValues[7]) / totalValues))
        avgPm10 = float("{:.2f}".format(float(dictValues[8]) / totalValues))
        avgDs18 = float("{:.2f}".format(float(dictValues[9]) / totalValues))
    
        samplesDict[key] = [avgTemp, avgCo2, avgRad, avgO3, avgNo2, avgCo, avgVoc, avgPm2_5, avgPm10, avgDs18, totalValues, "false"]

    return samplesDict


## Main function
def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer function fired.')

    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    dt = datetime.datetime.today()
    year = str(dt.year)
    month = str(dt.month)
    day = str(dt.day)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    deviceName = ""
    
    accountName = ""
    accountKey = ""

    # Retrieve devices from db
    devices = []
    table_name = 'AirSamples'
    table_service = None
    try:
        table_service = TableService(account_name=accountName, account_key=accountKey)
        entities = table_service.query_entities(table_name, filter = "PartitionKey eq 'Device'")
        for entity in entities:
            print(entity.DeviceName)
            devices.append(entity.DeviceName)
           
    except Exception as error:
        logging.info(error)

    # Get day
    dates = [
        {"year": year, "month": month, "day": day}
    ]

    # Request data from configured sensors
    for device in devices:
        for date in dates:
            # Build url in order to get data from sensor
            url = buildRequestUrl(device, date["year"], date["month"], date["day"])

            samplesDict = {}

            processedDate = StructuredDate()
            dailyAverages = DailyAverage()

            with requests.Session() as session:
                response = session.get(url)
                decodedResponse = response.content.decode('utf-8')

                # Getting csv
                responseCsv = csv.reader(decodedResponse.splitlines(), delimiter=',')
                listCsv = list(responseCsv)

                deviceName  = listCsv[0][0]

                # Remove first element (csv header)
                listCsv.pop(0)

                if len(listCsv) == 0:
                    continue

                # Parse csv
                parseCsv(listCsv, samplesDict, dailyAverages, processedDate)

                # Averaging daily values
                dailyAverages.averageValues(len(listCsv))

                # Averaging dict values (per hour)
                averageSamples(samplesDict)

                jsonDate = processedDate.year + '-' + processedDate.month + '-' + processedDate.day

                newDt = int(datetime.datetime.utcnow().replace(tzinfo=pytz.utc).timestamp())

                # Build the json as average daily values and average for each hour
                responseJson = buildJson(samplesDict, dailyAverages, jsonDate)

                rowKey = jsonDate

                query = Entity()
                query.PartitionKey = "AirSample"
                query.RowKey = str(newDt)
                query.DeviceName = deviceName
                query.SampleValues = responseJson

                # Insert value in db
                table_service.insert_entity('AirSamples', query)
                logging.info("Inserted entity in table storage")