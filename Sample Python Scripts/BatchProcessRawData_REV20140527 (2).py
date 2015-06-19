#!/usr/bin/env python
# Asssumes that the RawProcessData subroutine has been loaded into Python
import os, sys

#NEED TO: change myDir, uncomment appropriate s and change date (eg: CFADS2274-201XXXX)

myDir = "C:/Source/Mapping/GSV/C10293/"   #NOTE the car# is hardcoded in this line...
#s = "CFADS2274-20140"    # is car C10232
#s = "CFADS2280-20140"    # is car C10241
s = "CFADS2276-201408"    # is car C10293 
gZIP = 0       #1=gZip off, 0= gZip on

x1 = ""
for file in os.listdir(myDir):
    #print file
    if file.startswith(s):
        if file[:18] == x1:
            bFirst = False
        else:
            bFirst = True
        x1 = file[:18]
        
        #print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>"
        xCar = file[:9]
        xDate = file[10:18]
        #print "Running file: " + file + " xCar=" + str(xCar) + " xDate= " + str(xDate)
        theResult = ProcessRawData( xCar, xDate, myDir, file, bFirst, gZIP)
