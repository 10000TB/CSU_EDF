###
# Identify Peaks in Methane
# Written by Dave Theobald, following logic of Jessica Salo from "peak_algorithm_30July2013.py"
###
import os, sys, datetime, time, math
import csv, numpy


def IdentifyPeaks( xCar, xDate, xDir, xFilename):
    try:
        # "/Users/davidtheobald/Documents/projects/EDF_MethaneMapping/RawData/"
        #xDir = "/Users/davidtheobald/Documents/projects/EDF_MethaneMapping/RawData/"
        #xCar = "C10232"
        #xDate = "20121112"
        
        xABThreshold = 0.2                 # above baseline threshold above the mean value
        xDistThreshold = 20.0                 # find the maximum CH4 reading of observations within street segments of this grouping distance in meters
        xSDF = 1                    # multiplier times standard deviation for floating baseline added to mean
        xB = 240       # the number of records that constitutes the floating baseline time -- 7200 = 1 hour (assuming average of 0.5 seconds per record)
        #xCity = "FC"; xLonMin = -105.171; xLatMin = 40.465; xLonMax = -104.978; xLatMax = 40.653 # city abbreviation and bounding box in geographic coordinates
        xCity = "US"; xLonMin = -126.0; xLatMin = 24.5; xLonMax = -65.0; xLatMax = 50.0 # city abbreviation and bounding box in geographic coordinates
        
        #fn = xDir + xCar + "_" + xDate + ".csv"      #set raw text file to read in
        fn = xDir + "/" + xFilename      #set raw text file to read in
        fnOut = xDir + "Peaks" + "_" + xCar + "_" + xDate + ".csv"       #set CSV format output for observed peaks for a given car, day, city
        fnLog = xDir + "Peaks" + "_" + xCar + "_" + xDate + ".log"       #set CSV output for observed peaks for a given car, day, city
        fLog = open(fnLog, 'w')
        fnPoints = xDir + "Location" + "_" + xCar + ".csv"       #set CSV output for x,y locations of cars by days
        fPoints = open(fnPoints, 'a')
        
        #field column indices for various variables
        fFracDays = 2; fFracHours = 3; fEpochTime = 5; fAlarm = 6; fCavP = 8; fCavT = 9; fWarmBoxT = 12; fCH4 = 19; fLat = 22; fLon = 23
        
        #read data in from text file and extract desired fields into a list, padding with 5 minute and hourly average
        x1 = []; x2 = []; x3 = []; x4 = []; x5 = []; x6 = []; x7 = []
    
        print "1"
        count = 0
        cntOutside = 0
        with open(fn, 'rb') as f:
            t = csv.reader(f)
            for row in t:
                if count == 0:
                    print row
                    fieldName1 = row[fFracHours]; fieldName2 = row[fCH4]; fieldName3 = row[fLat]; fieldName4 = row[fLon]; 
                else:
                    x1.append(float(row[fFracHours])); x2.append(float(row[fCH4])); x3.append(float(row[fLat])); x4.append(float(row[fLon])); x5.append(0.0); x6.append(float(row[fEpochTime]))
                count += 1
        #print "Number of points outside the specified city's bounding box: " + str(cntOutside)
        print "Number of observations processed: " + str(count)
        #convert lists to numpy arrays
        aFracHours = numpy.array(x1); aCH4 = numpy.array(x2); aLat = numpy.array(x3); aLon = numpy.array(x4); aMean = numpy.array(x5); aEpochTime = numpy.array(x6)
        
        # find observations with CH4 greater than mean+t -- Above Baseline
        lstCH4_AB = []
        xCH4Mean = numpy.mean(aCH4[0:xB])       # initial floating baseline
        xCH4SD = numpy.std(aCH4[0:xB])
        if xCH4SD < (0.1 * xCH4Mean):
            xCH4SD = (0.1 * xCH4Mean)      # ensure that SD is at least ~0.2
        
        xLatMean = numpy.mean(aLat)
        xLonMean = numpy.mean(aLon)
        
        fLog.write ( "Day CH4_mean = " + str(numpy.mean(aCH4)) + ", Day CH4_SD = " + str(numpy.std(aCH4)) + "\n")
        fLog.write( "Center lon/lat = " + str(xLonMean) + ", " + str(xLatMean) + "\n")
        
        #fPoints.write(str(xCar) + "," + str(xDate) + "," + str(xLonMean) + "," + str(xLatMean) +"," + str(count) + "," + str(numpy.mean(aCH4)) + "," + str(xCH4SD) + ",POINT(" + str(xLonMean) + " " + str(xLatMean) + ")\n")
        
        #generate list of the index for observations that were above the threshold
        for i in range(0,count-2):
            # find floating baseline mean
            if ( i > xB):
                xCH4Mean = numpy.mean(aCH4[i-xB:i])
                xCH4SD = numpy.std(aCH4[i-xB:i])
                if xCH4SD < (0.1 * xCH4Mean):
                    xCH4SD = (0.1 * xCH4Mean)      # ensure that SD is at least ~0.2
            xThreshold = xCH4Mean + (xCH4SD * xSDF)
            if (aCH4[i] > xThreshold):
                lstCH4_AB.append(i)
                aMean[i] = xThreshold    #insert mean + SD as upper quartile CH4 value into the array to later retreive into the peak calculation
        
        # now group the above baseline threshold observations into groups based on distance threshold
        lstCH4_ABP = []
        lstPeakArea = []
        xDistPeak = 0.0
        xCH4Peak = 0.0
        xTime = 0.0
        cntPeak = 0
        cnt = 0
        cntStart = 0
        sID = ""
        sPeriod5Min = ""
        for i in lstCH4_AB:    
            if (cnt == 0):
                xLon1 = aLon[i]; xLat1 = aLat[i]
            else:
                # calculate distance between points
                xDist = math.sqrt(pow(((aLon[i]-xLon1) * (111319.9 * math.cos(math.radians(aLat[i])))), 2) + pow(((aLat[i]-xLat1) * 111319.9), 2))
                xDistPeak += xDist
                xCH4Peak += (xDist * (aCH4[i] - aMean[i]))
                xLon1 = aLon[i]; xLat1 = aLat[i]
                if (sID == ""):
                    xTime = aFracHours[i]
                    sID = str(xCar) + "_" + str(xDate) + "_" + str(xTime)
                    sPeriod5Min = str(int((aEpochTime[i] - 1350000000) / (60 * 5)))
                if (xDist > xDistThreshold):       #initial start of a observed peak
                    ##calc maximum value for a peak
                    #if (xDistPeak > 160.0):
                    #    xType = 0
                    #else:
                    #    xType = 1
                    #
                    #for j in range(cntStart, i):   
                    #    lstPeakArea.append(xType)
                    cntPeak += 1
                    xTime = aFracHours[i]
                    xDistPeak = 0.0
                    xCH4Peak = 0.0
                    sID = str(xCar) + "_" + str(xDate) + "_" + str(xTime)
                    sPeriod5Min = str(int((aEpochTime[i] - 1350000000) / (60 * 5)))
                    #print str(i) +", " + str(xDist) + "," + str(cntPeak) +"," + str(xDistPeak)           
                 
        #        lstCH4_ABP.append([sID, xTime, aFracHours[i], aCH4[i], aLat[i], aLon[i], aMean[i], xDistPeak, xCH4Peak])
                lstCH4_ABP.append([sID, xTime, aFracHours[i], aCH4[i], "POINT(" + str(aLon[i]) + " " + str(aLat[i]) + ")", aLon[i],aLat[i],aMean[i], xDistPeak, xCH4Peak, sPeriod5Min])
            cnt += 1
        
        fLog.write ( "Number of peaks found: " + str(cntPeak) + "\n")
        fPoints.write(str(xCar) + "," + str(xDate) + "," + str(xLonMean) + "," + str(xLatMean) +"," + str(count) + "," + str(numpy.mean(aCH4)) + "," + str(xCH4SD) + "," + str(cntPeak) + ",POINT(" + str(xLonMean) + " " + str(xLatMean) + ")\n")
        print xCar + "\t" + xDate + "\t" + xFilename + "\t" + str(count) + "\t" + str(cntPeak)
        #### calculate attribute for the area under the curve -- PPM
        
        #write out the observed peaks to a csv to be read into a GIS
        fOut = open(fnOut, 'w')
        #s = "PEAK_NUM,FRACHRSTART," + fieldName1 + "," + fieldName2 + "," + fieldName3 + "," + fieldName4 + ',CH4_MEAN,PEAK_DIST_M,PEAK_CH4\n'
        s = "PEAK_NUM,FRACHRSTART," + fieldName1 + "," + fieldName2 + ",WKT" + ',LON,LAT,CH4_BASELINE,PEAK_DIST_M,PEAK_CH4,PERIOD5MIN\n'
        fOut.write(s)
        for r in lstCH4_ABP:
            s = ''
            for rr in r:
                s += str(rr) + ','
            s = s[:-1]
            s += '\n'
            #print str(s)
            fOut.write(s)
        
        fOut.close()
        fLog.close()
        fPoints.close()
        
        return True
    except ValueError:
        print "Error in Identify Peaks"
        return False
