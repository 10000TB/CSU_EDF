#!/usr/bin/env python
# Asssumes that the RawProcessData subroutine has been loaded into Python
import os, sys

#NEED TO: modify myDir, uncomment appropriate s's and change date, and chage s in "if file.startswith(sX)

myDir = "C:/Data/FortCollins/NewDataProcessing_140804/"
#s1 = "CFADS2274_2014"    # is car C10232
s2 = "CFADS2280_20140"    # is car C10241
#s3 = "CFADS2276_201"    # is car C10293

for file in os.listdir(myDir):
    #print file
    if file.startswith(s2):
        #print file[19:22]
        if file[19:22] == "dat":
            #print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
            xCar = file[:9]
            xDate = file[10:18]
            #print "Running file: " + file + " xCar=" + str(xCar) + " xDate= " + str(xDate)
            theResult = IdentifyPeaks( xCar, xDate, myDir, file)
