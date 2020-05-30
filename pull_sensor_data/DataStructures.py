## Class used to mantain averages values
class DailyAverage:
    def __init__(self):
        self.avgDailyTemp    = 0
        self.avgDailyCo2     = 0
        self.avgDailyRad     = 0
        self.avgDailyO3      = 0
        self.avgDailyNo2     = 0
        self.avgDailyCo      = 0
        self.avgDailyVoc     = 0
        self.avgDailyPm2_5   = 0
        self.avgDailyPm10    = 0
        self.avgDailyDs18    = 0
        self.avgLatitude     = 0
        self.avgLongitude    = 0

    def averageValues(self, valuesNumber):
        float("{:.2f}".format(self.avgDailyTemp / valuesNumber))

        self.avgDailyTemp   = float("{:.2f}".format(self.avgDailyTemp / valuesNumber))
        self.avgDailyCo2    = float("{:.2f}".format(self.avgDailyCo2 / valuesNumber))
        self.avgDailyRad    = float("{:.2f}".format(self.avgDailyRad / valuesNumber)) 
        self.avgDailyO3     = float("{:.2f}".format(self.avgDailyO3 / valuesNumber))   
        self.avgDailyNo2    = float("{:.2f}".format(self.avgDailyNo2 / valuesNumber))  
        self.avgDailyCo     = float("{:.2f}".format(self.avgDailyCo / valuesNumber))   
        self.avgDailyVoc    = float("{:.2f}".format(self.avgDailyVoc / valuesNumber))   
        self.avgDailyPm2_5  = float("{:.2f}".format(self.avgDailyPm2_5 / valuesNumber))   
        self.avgDailyPm10   = float("{:.2f}".format(self.avgDailyPm10 / valuesNumber))  
        self.avgDailyDs18   = float("{:.2f}".format(self.avgDailyDs18 / valuesNumber))   
        self.avgLatitude    = float("{:.7f}".format(self.avgLatitude / valuesNumber))   
        self.avgLongitude   = float("{:.7f}".format(self.avgLongitude / valuesNumber))   


## Class used to maintain a structured date
class StructuredDate:
    def __init__(self):
        self.year    = 0
        self.month   = 0
        self.day     = 0
        self.hour    = 0