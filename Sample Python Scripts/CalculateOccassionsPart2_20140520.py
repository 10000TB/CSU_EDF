import arcpy
from arcpy.sa import *
from arcpy import env

sDir = "C:/Data/Texas/ProcessedData_140721/"
sCity = "TEX"

fnRoads = sDir + sCity + "_RoadPoints_140722X.shp"
fnObs = sDir + sCity + "_AllOccasions140722.shp"
fnOutTable = sDir + "PD_" + sCity + "_roads_v_occ.dbf"
#fnOutTable = sDir + "test.dbf"

arcpy.PointDistance_analysis(fnRoads,fnObs,fnOutTable,"20 Meters")

# join
arcpy.JoinField_management(fnOutTable,"NEAR_FID",fnObs,"FID","Period5Min")
#calculate time period (every 5 minutes is a new one...)
#arcpy.AddField_management(fnOutTable,"Period5Min","LONG","#","#","#","#","NULLABLE","NON_REQUIRED","#")
#arcpy.CalculateField_management(fnOutTable,"Period5Min","([EPOCH_TIME] - 1350000000) / (60 * 5)","VB","#")# convert to point at very fine resolution to allow different passes to pop through by chance

print "Calculating number of occasions..."
lstOut = []
lstID = []
xOldRoadID = -999
lstV = []
cursor = arcpy.SearchCursor(fnOutTable)
for row in cursor:
    xRoadID = row.getValue("Input_FID")
    if xRoadID != xOldRoadID:
        xOldRoadID = xRoadID
        #print str(xRoadID) + ", " + str(len(set(lstV)))
        lstOut.append(len(set(lstV)))       #get the count of unique values in the list, equals # of observations/passes
        lstV = []
        lstID.append(xRoadID)
    lstV.append(row.getValue("Period5Min"))
del row, cursor

print "Assigning number of occasions back to road points..."
#assign occasions back to road points
arcpy.AddField_management(fnRoads,"NumOccs","LONG","#","#","#","#","NULLABLE","NON_REQUIRED","#")
cursor = arcpy.UpdateCursor(fnRoads)
i = 0
j = len(lstID)
for row in cursor:
    if i < j:
        x1 = row.getValue("FID")
        #print str(x1) + ", " + str(i)
        if lstID[i] == x1:
            row.setValue("NumOccs", lstOut[i])
            cursor.updateRow(row)
            i += 1
del row, cursor

