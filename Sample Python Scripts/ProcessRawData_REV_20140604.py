###
# Process raw data files, to:
# 1. combine by unique car and day combination
# 2. add car number as a field to the processed data...
# 3. data QA/QC steps:
#   a. remove if >45 mph
#   b. check for parameters out of specification for Picarro instrument
#   c. bad entries such as car speed: 1.#QNAN00000E+000 or -1.#IND000000E+000 or blank lines **send message to signify what file blank lines are in...
#   d. check for lat/long values of 0.0
#   e. adjusts x,y location for estimated time delay due to outlet pressure changes
# 4. write out as .csv to reduce file size and make ingesting into ArcGIS easier
# Written by Dave Theobald, following logic of Jessica Salo from "raw_data_processing.py"
###
import os, sys, datetime, time, math, csv, numpy, gzip

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

#NEED TO: change xOutDir
    
def ProcessRawData( xCar, xDate, xDir, xFilename, bFirst, gZIP):
    try:
        #print "Processing Raw Data on: " + xCar +", " + xDate
        
        #assumes Python started in Terminal located in root directory: 
        # "/Users/davidtheobald/Documents/projects/EDF_MethaneMapping/RawData/C10241/"
        #xOutDir = "C:/Source/Mapping/GSV/C10241/"
        xOutDir = "C:/Data/LA/CoverageChecks/ProcessedRawData/"
        #car C10232 = CFADS2274;
        ###xCar = "C10232"
        ###xDate2 = "20121112"
        #gZIP = 0    #1 = true, 0 equals false
        
        xMaxCarSpeed = 45.0 / 2.23694     # assumes 45 mph as max, conver to meters per second       
        #data quality thresholds
        #xCavPL = 140.0 - 0.1; xCavPH = 140.0 + 0.1; xCavTL = 45.0 - 0.005; xCavTH = 45.0 + 0.005; xWarmBoxTL = 45.0 - 0.005; xWarmBoxTH = 45.0 + 0.005  # from Kathy McCain
        xCavPL = 140.0 - 1; xCavPH = 140.0 + 1; xCavTL = 45.0 - 1; xCavTH = 45.0 + 1; xWarmBoxTL = 45.0 - 1; xWarmBoxTH = 45.0 + 1  #quality thresholds (relaxed)
        
        #set up outlet-pressure delay parameters, specific to each car/instrument
        if xCar == "CFADS2274":
            # is car C10232
            xX2 = 0.00000006; xX1 = 0.0031; xX0 = 46.711; xOutletPressureLow = 19000; xOutletPressureHigh = 27000; xOutletPressureHighValue = 5.5; xDelay = 1.5
        elif xCar == "CFADS2280":
            # is car C10241
            xX2 = 0.00000004; xX1 = 0.0036; xX0 = 82.483; xOutletPressureLow = 32000; xOutletPressureHigh = 39000; xOutletPressureHighValue = 6.3; xDelay = 1.5   
        elif xCar == "CFADS2276":
            # is car C10293
            xX2 = 0.00000008; xX1 = 0.0044; xX0 = 61.386; xOutletPressureLow = 19000; xOutletPressureHigh = 24000; xOutletPressureHighValue = 4.8; xDelay = 1.5   
        
            
        # set header for fields
        #          0    1    2                    3                   4           5          6            7           8              9          10      11         12          13      14          15          16              17  18      19  20      21  22          23           24      25          26          27             28             29     30     31            32          33        34     35
        sHeader = 'DATE,TIME,FRAC_DAYS_SINCE_JAN1,FRAC_HRS_SINCE_JAN1,JULIAN_DAYS,EPOCH_TIME,ALARM_STATUS,INST_STATUS,CavityPressure,CavityTemp,DasTemp,EtalonTemp,WarmBoxTemp,species,MPVPosition,OutletValve,solenoid_valves,CO2,CO2_dry,CH4,CH4_dry,H2O,GPS_ABS_LAT,GPS_ABS_LONG,GPS_FIT,WS_WIND_LON,WS_WIND_LAT,WS_COS_HEADING,WS_SIN_HEADING,WIND_N,WIND_E,WIND_DIR_SDEV,WS_ROTATION,CAR_SPEED,CAR_ID,WKT\n'
        sHisHeader = 'CAR,DATE,CavPressMean,CavPressSTD,CavTempMean,CavTempSTD,WarmBTMean,WarmBTSTD,OutletPMean,OutletPSTD,CH4Mean,CH4STD,CarVelocityMean,CarVelocitySTD,WIND_NMean,WIND_EMean\n'
        # compile summary histogram data
        x8 = []; x9 = []; x12 = []; x15 = []; x19 = []; x22 = []; x23 = []; x30 = []; x31 = []; x33 = [];
        
        #get all the files in the subdirectory
        #xDir2 = xDir + xCar         #+ "/" + xDate + "/"
        if gZIP == 0:
            f = gzip.open(xDir + "/" + xFilename, 'rb')
        else:
            f = open(xDir + "/" + xFilename, 'rb')
        # process    
        #if first time on this car/date, then write header out
        fnOut = xOutDir + xCar + "_" + xDate + "_dat.csv"       #set CSV output for raw data
        fnLog = xOutDir + xCar + "_" + xDate + "_log.csv"       #output for logfile
        fnHis = xOutDir + xCar + "_" + xDate + "_his.csv"       #set histogram output for raw data
        if bFirst:
            fOut = open(fnOut, 'w')
            fOut.write(sHeader)
            fLog = open(fnLog, 'w')
            fHis = open(fnHis, 'w')
            fHis.write(sHisHeader)
            print "fnLog: "+fnOut
        else:
            fOut = open(fnOut, 'a')
            fLog = open(fnLog, 'a')
            fHis = open(fnHis, 'a')
        
        #read all lines
        xCntObs = 0
        xCntGoodValues = 0
        iDelay = 0
        for row in f:
            bGood = True
            s1 = ""
            for i in range(0,34):
                xStart = i * 26
                xEnd = xStart + 26
                s2 = row[xStart:xEnd].replace(" ","")
                s1 += s2 + ","
                if (i > 1):
                    if not is_number(s2):
                        bGood = False
                        #print "Not a number"
            if bGood:
                lstS = s1.split(",")
                # get raw values to summarize over each file, including GPS locations (22 = lat, 23 = long)
                x8.append(float(lstS[8])); x9.append(float(lstS[9])); x12.append(float(lstS[12])); x15.append(float(lstS[15])); x19.append(float(lstS[19])); x22.append(float(lstS[22])); x23.append(float(lstS[23])); x30.append(float(lstS[30])); x31.append(float(lstS[31])); x33.append(float(lstS[33]));
                
                #test for proper real values
                if float(lstS[19]) < 1.5:
                    fLog.write("CH4 value less than 1.5: "+ str(lstS[19]) + "\n")
                    continue
                if float(lstS[33]) > xMaxCarSpeed:
                    fLog.write("Car speed of " + str(float(lstS[33])) + " exceeds max threshold of: " + str(xMaxCarSpeed) + "\n")
                    continue
                #RSSI applies only to CSU vehicle
                #if float(lstS[28]) < 45.0:
                #    fLog.write("RSSI value less than 45: " + lstS[28] + "\n")
                #    continue
                
                if float(lstS[8]) < xCavPL:
                    fLog.write("Cavity Pressure " + str(lstS[8]) + " outside of range: " + str(xCavPL) + "\n")
                    continue
                if float(lstS[8]) > xCavPH:
                    fLog.write("Cavity Pressure " + str(float(lstS[8])) + " outside of range: " + str(xCavPH) + "\n")
                    continue
                if float(lstS[9]) < xCavTL:
                    fLog.write("Cavity Temperature " + str(float(lstS[9])) + " outside of range: " + str(xCavTL) + "\n")
                    continue
                if float(lstS[9]) > xCavTH:
                    fLog.write("Cavity Temperature " + str(float(lstS[9])) + " outside of range: " + str(xCavTH) + "\n")
                    continue
                if float(lstS[12]) < xWarmBoxTL:
                    fLog.write("Warm Box Temperature " + str(float(lstS[12])) + " outside of range: " + str(float(xWarmBoxTL)) + "\n")
                    continue
                if float(lstS[12]) > xWarmBoxTH:
                    fLog.write("Warm Box Temperature " + str(float(lstS[12])) + " outside of range: " + str(float(xWarmBoxTH)) + "\n")
                    continue
                #test for outlet pressure and adjust delay time/location based on function
                xOutletPressure = float(lstS[15])
                if xOutletPressure < xOutletPressureLow:
                    fLog.write("Outlet Pressure " + str(xOutletPressure) + " below minimum value: " + str(xOutletPressureLow) + "\n")
                    xTimeDelay = 0  ###????
                    continue
                elif xOutletPressure > xOutletPressureHigh:
                    xTimeDelay = xOutletPressureHighValue - xDelay
                else:
                    xTimeDelay = (xX2 * xOutletPressure * xOutletPressure) + (xX1 * xOutletPressure) + xX0 - xDelay
                ###
                
                #adjust the x,y location based on time delay. Assume 2 observations per second. Need to store the x,y locations in an array to access back in time.
                ####################################################################
                iDelay = xCntGoodValues - int(xTimeDelay * 2)
                if iDelay < 0:      # check to see if one of the beginning points... delay can't be before the first point
                    iDelay = 0
                s1 += xCar +",POINT(" + str(float(x23[iDelay])) + " " + str(float(x22[iDelay])) + ")\n"
                fOut.write(s1[:-1] + "\n")
                xCntGoodValues += 1
            xCntObs += 1
            #print str(xCntObs) + ", " + str(xCntGoodValues) + ", " + str(iDelay)
        #$sOut = "From " + str(f) + " Read " + str(xCntObs) + " lines in, wrote "+ str(xCntGoodValues) + " lines out\n"
        sOut = str(gZIP) + "," + str(f) + "," + str(xCntObs) + "," + str(xCntGoodValues) + "\n"
        fLog.write(sOut)
               
        fOut.close()
        fLog.close()
        
        #summarize info for histogram
        a8 = numpy.array(x8); a9 = numpy.array(x9); a12 = numpy.array(x12); a15 = numpy.array(x15); a19 = numpy.array(x19); a30 = numpy.array(x30); a31 = numpy.array(x31); a33 = numpy.array(x33);
        sHis = xCar + "," + xDate + "," + str(numpy.mean(a8)) + "," + str(numpy.std(a8)) + "," + str(numpy.mean(a9)) + "," + str(numpy.std(a9)) + "," + str(numpy.mean(a12)) + "," + str(numpy.std(a12)) + "," + str(numpy.mean(a15)) + "," + str(numpy.std(a15)) + "," + str(numpy.mean(a19)) + "," + str(numpy.std(a19)) + "," + str(numpy.mean(a33)) + "," + str(numpy.std(a33)) + "," + str(numpy.mean(a30)) + "," + str(numpy.mean(a31)) + "\n" 
        fHis.write(sHis)
        fHis.close()
        #print xCar + "," + xDate + "," + xFilename + "," + str(xCntObs) + "," + str(xCntGoodValues) + "," + str(gZIP)
        print xCar + "\t" + xDate + "\t" + xFilename + "\t" + str(xCntObs) + "\t" + str(xCntGoodValues) + "\t" + str(gZIP)
        
        return True
    except ValueError:
        return False
