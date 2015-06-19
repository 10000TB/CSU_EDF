import arcpy
from arcpy.sa import *
from arcpy import env

#NEED TO: modify sDir, sCity, and sObs

sDir = "C:/Data/FortCollins/NewDataProcessing_140804/"
#sDirOut = "C:/Users/davet/projects/EDF_Methane/Boston/"
sCity = "FTC"   
sObs = "CFADS2280_" #have to repeat for each car

#NEED TO: uncomment appropriate date list(s)
# Note multiple lines for same car, last of which exluces dates that didn't work

#list of dates for Fort Collins for mapping, minus the test days
#lstCFADS2274 = [20121112, 20121113, 20121115, 20121116, 20121119, 20121120, 20121121, 20121126, 20121127, 20121128, 20121129, 20121218, 20121221, 20131007, 20131008, 20131009 ]
#lstCFADS2274 = [20121112, 20121113, 20121115, 20121116, 20121119, 20121120, 20121121, 20121126, 20121127, 20121128, 20121129, 20121218, 20121221 ]
#lstCFADS2280 = [20121218,20130107, 20130108, 20130930, 20131001, 20131002, 20131003, 20131007, 20131008, 20131009, 20131010, 20131011, 20131015, 20131015, 20131105, 20131220, 20131227, 20140108, 20140130, 20140225, 20140227, 20140228, 20140303, 20140305, 20140306, 20140310, 20140312, 20140313, 20140317, 20140318, 20140325, 20140326, 20140327, 20140331, 20140401, 20140402, 20140403, 20140404, 20140407, 20140408]
#lstCFADS2280 = [20140225, 20140227, 20140228, 20140303, 20140305, 20140306, 20140310, 20140312, 20140313, 20140317, 20140318, 20140325, 20140326, 20140327, 20140331, 20140401, 20140402, 20140403, 20140404, 20140407, 20140408]

#"New" Fort Collins Data (7/11/14)
lstCFADS2280 = [20140514, 20140515, 20140516, 20140522, 20140523, 20140527, 20140528, 20140529, 20140530, 20140602, 20140603, 20140604, 20140605, 20140606, 20140609, 20140610, 20140611, 20140613, 20140616, 20140618, 20140619, 20140620, 20140623, 20140625, 20140630, 20140701, 20140702, 20140709, 20140721, 20140722, 20140725, 20140728, 20140729]
    
#list of dates for Indy
#lstCFADS2274 = [20130610, 20130611, 20130612, 20130613]
#lstCFADS2280 = [20130610, 20130611, 20130613, 20130614, 20130617, 20130618, 20130619, 20130620, 20130621, 20130624, 20130625, 20130626, 20130627, 20130628, 20130701, 20130702]
#Removed 20130611, 20130619, 20130624, 20130626, 20130702
#lstCFADS2280 = [20130610, 20130613, 20130614, 20130617, 20130618, 20130620, 20130621, 20130625, 20130627, 20130628, 20130701]

#list of dates for Denver
#lstCFADS2274 = [20121203, 20121204, 20121205, 20121206, 20121207, 20121210, 20121211, 20121212, 20121213, 20121214, 20121217, 20121227, 20121228, 20130103, 20130104, 20130107, 20130108, 20130114, 20130115, 20131007, 20131008, 20131009, 20131010, 20131011, 20131014, 20131015, 20131016, 20131017, 20131021, 20131022, 20131023, 20131024, 20131025, 20131028, 20131029, 20131030, 20131101 ]
#lstCFADS2276 = [20121112, 20121116, 20121119, 20121120, 20121127, 20121128, 20121129, 20121130, 20121203, 20121204, 20121205, 20121206, 20121207, 20121210, 20121211, 20121212, 20121213, 20121214, 20121217, 20121218, 20130102, 20130103, 20130104, 20130910, 20130911, 20130916, 20130917, 20130918, 20130919, 20130920, 20130923, 20130924, 20130925, 20130926, 20130930, 20131001, 20131002, 20131003, 20131007, 20131008, 20131009 ]
#lstCFADS2280 = [20121115, 20121116, 20121119, 20121120, 20121121, 20121126, 20121127, 20121128, 20121129, 20121130, 20121203, 20121204, 20121205, 20121206, 20121207, 20121210, 20121211, 20121212, 20121214, 20121217, 20130114, 20130115, 20131011, 20131014, 20131015, 20131016, 20131017, 20131021, 20131022, 20131023, 20131024, 20131025, 20131029, 20131030, 20131031, 20131101, 20131105, 20131106, 20131111, 20131112, 20131113, 20131114, 20131115, 20131118, 20131119, 20131120, 20131125, 20131126, 20131127, 20131202, 20131203, 20131205, 20131206, 20131210, 20131211, 20131217, 20131218, 20131219, 20131230, 20140102, 20140103, 20140106, 20140107, 20130110, 20140113, 20140115, 20140116, 20140117, 20140120, 20140121, 20140122, 20140124 ]

#list of dates for Staten Island
#lstCFADS2276 = [20131125, 20131126, 20131202, 20131203, 20131204, 20131205, 20131211, 20131212, 20131213, 20131216, 20131218, 20131219, 20131220, 20131227, 20140115, 20140116, 20140117, 20140120, 20140127, 20140128, 20140130, 20140131, 20140206, 20140207, 20140208, 20140210, 20140211, 20140212, 20140214, 20140217, 20140219, 20140220, 20140304, 20140305, 20140306, 20140307, 20140310, 20140311, 20140312, 20140313, 20140317, 20140318, 20140319, 20140320, 20140321, 20140324, 20140325, 20140327, 20140328, 20140331, 20140401, 20140402, 20140403, 20140407, 20140410, 20140411, 20140414 ]
#lstCFADS2276 = [20140403, 20140407, 20140410, 20140411, 20140414 ]

#list of dates for Boston
#lstCFADS2274 = [20130307, 20130311, 20130312, 20130313, 20130314, 20130315, 20130318, 20130320, 20130321, 20130322, 20130325, 20130327, 20130328, 20130401, 20130402, 20130404, 20130405, 20130408, 20130409, 20130411, 20130415, 20130416, 20130423, 20130424, 20130425, 20130426, 20130503, 20130506, 20130507, 20130508, 20130509, 20130510, 20130513, 20130514, 20130515, 20130516, 20130520, 20130521, 20130523, 20130524, 20130528, 20130529, 20130530, 20130531, 20130604, 20130605, 20130606]
#lstCFADS2276 = [20130307, 20130311, 20130312, 20130313, 20130314, 20130315, 20130318, 20130319, 20130320, 20130322, 20130325, 20130326, 20130327, 20130328, 20130329, 20130401, 20130402, 20130403, 20130411, 20130417, 20130418, 20130419, 20130422, 20130423, 20130424, 20130426, 20130429, 20130430, 20130501, 20130503, 20130506, 20130507, 20130508, 20130509, 20130510, 20130513, 20130514, 20130515, 20130516, 20130603, 20130604, 20130605, 20130606, 20130607, 20130610, 20130611, 20130612, 20130613, 20130614, 20130617, 20130618, 20130619, 20130620, 20130621, 20130624, 20130625, 20130626 ]
#lstCFADS2280 = [20130301, 20130304, 20130306, 20130307, 20130312, 20130314, 20130409, 20130502, 20130503, 20130506, 20130507, 20130508, 20130509, 20130510, 20130513, 20130514, 20130606 ]
#Removed 20130305, 20130311, 20130313, 20130315, 20130318, 20130319, 20130320, 20130321, 20130322, 20130325, 20130408, 20130410, 20130411, 20130412, 20130415, 20130416, 20130417, 20130423, 20130424, 20130425, 20130426, 20130429, 20130430, 

#list of dates for Syracuse
#lstCFADS2274 = [20140428, 20140429, 20140430, 20140501, 20140502, 20140505, 20140506, 20140507, 20140508, 20140509, 20140512, 20140513, 20140514, 20140515, 20140519, 20140520, 20140521, 20140522, 20140523, 20140527, 20140528, 20140529, 20140530, 20140602, 20140603, 20140604, 20140605, 20140606, 20140609, 20140610, 20140616, 20140617, 20140619, 20140620, 20140623, 20140626, 20140627, 20140630, 20140718, 20140721, 20140722, 20140723, 20140724, 20140725, 20140729, 20140730, 20140731 ]
#Removed: 20140506, 20140522
#lstCFADS2274 = [20140428, 20140429, 20140430, 20140501, 20140502, 20140505, 20140507, 20140508, 20140509, 20140512, 20140513, 20140514, 20140515, 20140519, 20140520, 20140521, 20140523, 20140527, 20140528, 20140529, 20140530, 20140602, 20140603, 20140604, 20140605, 20140606, 20140609, 20140610, 20140616, 20140617, 20140619, 20140620, 20140623, 20140626, 20140627, 20140630, 20140718, 20140721, 20140722, 20140723, 20140724, 20140725, 20140729, 20140730, 20140731 ]

#Christman Release (dates and car not accurate)
#lstCFADS2280 = [20140616]

#list of dates for Texas
#lstCFADS2276 = [20131015, 20131016, 20131017, 20131018, 20131021, 20131022, 20131023, 20131024, 20131025, 20131028, 20131029] 
#Removwed 20131016, 20131029
#lstCFADS2276 = [20131015, 20131017, 20131018, 20131021, 20131022, 20131023, 20131024, 20131025, 20131028] 

#NEED TO: uncomment appropriate car

#make peaks shapefiles
#for i in lstCFADS2274:
#for i in lstCFADS2276:
for i in lstCFADS2280:
    fnIn = sDir + "Peaks_" + sObs + str(i)
    fnOut = sDir + sCity + "Peaks_" + sObs + str(i)
    print fnIn + " peaks..."
    arcpy.MakeXYEventLayer_management(fnIn + ".csv","lon","lat",sObs + "L","GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119522E-09;0.001;0.001;IsHighPrecision","#")
    arcpy.FeatureToPoint_management(sObs + "L",fnOut + ".shp","CENTROID")


#### MANUAL merge these files into "<city>_LeakPoints"
    
# now generate static leaks (aka verified peaks)
#summarize on Peak_Num and calculate stats, mean, std, min, etc.
fnLeakPoints = sDir + sCity + "_LeakPoints.shp"
fnLeakPointsBuff = sDir + sCity + "LeakPointsBuff.shp"
fnLeakPointsSum = sDir + sCity + "LeakPointsSum.dbf"
fnLeakPointsCentroids = sDir + sCity + "AllLeakCentroids.shp"
fnStaticLeakPoints = sDir + sCity + "StaticLeakPoints.shp"

fnStaticLeakPointsBuff = sDir + sCity + "StaticLeakPointsBuff.shp"
fnStaticLeakPointsBuff2 = sDir + sCity + "StaticLeakPointsBuff2.shp"
fnStaticLeakPoints2Sum = sDir + sCity + "StaticLeakPointsSum2.dbf"
fnStaticLeakVerified = sDir + sCity + "StaticLeakBuffCentroids.dbf"
fnStaticLeakVerifiedFinal = sDir + sCity + "StaticLeakBuffCentroidsFinal.dbf"

# 1. summarize point attributes to each unique leak
arcpy.Statistics_analysis(fnLeakPoints,fnLeakPointsSum,"CH4_BASELI MIN;PEAK_DIST_ MAX;PEAK_CH4 MAX;CH4 MEAN;CH4 STD;CH4 MAX;CH4 MIN;PEAK_DIST_ MAX","PEAK_NUM") #summarize points for each peak num
arcpy.Buffer_analysis(fnLeakPoints,fnLeakPointsBuff,"20 Meters","FULL","ROUND","LIST","PEAK_NUM")   #find points of peaks that are within 20 m and dissolve on Peak Number
arcpy.JoinField_management(fnLeakPointsBuff,"PEAK_NUM",fnLeakPointsSum,"PEAK_NUM","FREQUENCY;MIN_CH4_BA;MAX_PEAK_D;MAX_PEAK_C;MEAN_CH4;STD_CH4;MAX_CH4;MIN_CH4")   #join summarized stats to buffered peaks

# 2. Leak buffers to centroids
arcpy.FeatureToPoint_management(fnLeakPointsBuff,fnLeakPointsCentroids,"INSIDE")    #convert buffered peaks to single point at the centroid
arcpy.AddField_management(fnLeakPointsCentroids,"PPMM","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")    #add field to calculate PPMM - CH4 parts per million per meter
arcpy.CalculateField_management(fnLeakPointsCentroids,"PPMM","[MAX_PEAK_D] * ([MEAN_CH4] - [MIN_CH4_BA])","VB","#")    # calculate PPMM as a function of (mean CH4 - baseline ) * distance of peak
arcpy.AddField_management(fnLeakPointsCentroids,"D","TEXT","10")
arcpy.CalculateField_management(fnLeakPointsCentroids,"D","Right(Left( [PEAK_NUM], 18), 8)","VB","#")   # calculate using yyyymmdd format
#arcpy.CalculateField_management(fnLeakPointsCentroids,"Date","""!PEAK_NUM![14:16] + "/" + !PEAK_NUM![16:18] + "/" + !PEAK_NUM![10:14] ""","PYTHON","#")     # calculate using mm/dd/yyyy format

# 3. Leak centroids to StaticLeakCentroid
arcpy.Select_analysis(fnLeakPointsCentroids,fnStaticLeakPoints,""""MAX_PEAK_D" > 0.1 AND "MAX_PEAK_D" < 160 """)  #remove peaks that have a single point because they have distance = 0, and remove dist > 160 m because likely not a static leak source (i.e. AREA peak)
arcpy.AddField_management(fnStaticLeakPoints,"FlowRate","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")
arcpy.CalculateField_management(fnStaticLeakPoints,"FlowRate","10.0**(0.06505 + (0.06925 * !MAX_CH4!) + (-0.004516 * !PPMM!) + (!MAX_CH4! - 8.28999) * ((!PPMM! - 137.6) * 0.000051747) + (0.081365 * (!PPMM!  / !MAX_CH4!)))","PYTHON","#")

# 4. Static leak centroids
arcpy.Buffer_analysis(fnStaticLeakPoints,fnStaticLeakPointsBuff,"20 Meters","FULL","ROUND","ALL","#")
arcpy.MultipartToSinglepart_management(fnStaticLeakPointsBuff,fnStaticLeakPointsBuff2)
arcpy.CalculateField_management(fnStaticLeakPointsBuff2,"ORIG_FID","[FID]","VB","#")

# 5. Manually spacially join fnStaticLeakPoints and fnStaticLeakPointsBuff2
#Save into same sDir directory as fnStaticLeakPoints2
#arcpy.SpatialJoin_analysis(fnStaticLeakPoints,fnStaticLeakPointsBuff2,fnStaticLeakPoints2,"JOIN_ONE_TO_ONE","KEEP_ALL","""PEAK_NUM "PEAK_NUM" true true false 254 Text 0 0 ,First,#,FTC_StaticLeakPoints,PEAK_NUM,-1,-1;FREQUENCY "FREQUENCY" true true false 9 Long 0 9 ,First,#,FTC_StaticLeakPoints,FREQUENCY,-1,-1;MIN_CH4_BA "MIN_CH4_BA" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,MIN_CH4_BA,-1,-1;MAX_PEAK_D "MAX_PEAK_D" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,MAX_PEAK_D,-1,-1;MAX_PEAK_C "MAX_PEAK_C" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,MAX_PEAK_C,-1,-1;MEAN_CH4 "MEAN_CH4" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,MEAN_CH4,-1,-1;STD_CH4 "STD_CH4" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,STD_CH4,-1,-1;MAX_CH4 "MAX_CH4" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,MAX_CH4,-1,-1;MIN_CH4 "MIN_CH4" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,MIN_CH4,-1,-1;ORIG_FID "ORIG_FID" true true false 9 Long 0 9 ,First,#,FTC_StaticLeakPoints,ORIG_FID,-1,-1;PPMM "PPMM" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,PPMM,-1,-1;Date "Date" true true false 9 Long 0 9 ,First,#,FTC_StaticLeakPoints,Date,-1,-1;FlowRate "FlowRate" true true false 19 Double 0 0 ,First,#,FTC_StaticLeakPoints,FlowRate,-1,-1;Id "Id" true true false 6 Long 0 6 ,First,#,FTC_StaticLeakPointsBuff2,Id,-1,-1;ORIG_FID_1 "ORIG_FID_1" true true false 9 Long 0 9 ,First,#,FTC_StaticLeakPointsBuff2,ORIG_FID,-1,-1""","INTERSECT","#","#")

# 6. Clusters of leaks (verified peaks)
#fnStaticLeakPoints2 = sDir + "fnStaticLeakPoints2.shp"
fnStaticLeakPoints2 = sDir + "fnStaticLeakPoints2.shp"

arcpy.Statistics_analysis(fnStaticLeakPoints2,fnStaticLeakPoints2Sum,"D FIRST;D LAST;PPMM MEAN;MAX_CH4 MEAN;MIN_CH4_BA MEAN;FlowRate MEAN","ORIG_FID_1")
arcpy.FeatureToPoint_management(fnStaticLeakPointsBuff2,fnStaticLeakVerified,"INSIDE")    #convert buffered peaks to single point at the centroid
arcpy.JoinField_management(fnStaticLeakVerified,"ORIG_FID",fnStaticLeakPoints2Sum,"ORIG_FID_1","FREQUENCY;FIRST_D;LAST_D;MEAN_PPMM;MEAN_MAX_C;MEAN_MIN_C;MEAN_FlowR")
arcpy.AddField_management(fnStaticLeakVerified,"First_Date","TEXT","10")
arcpy.AddField_management(fnStaticLeakVerified,"Last_Date","TEXT","10")
arcpy.CalculateField_management(fnStaticLeakVerified,"First_Date","""!First_D![4:6] + "/" + !First_D![6:8] + "/" + !First_D![0:4] ""","PYTHON","#")     # calculate using mm/dd/yyyy format
arcpy.CalculateField_management(fnStaticLeakVerified,"Last_Date","""!Last_D![4:6] + "/" + !Last_D![6:8] + "/" + !Last_D![0:4] ""","PYTHON","#")     # calculate using mm/dd/yyyy format
arcpy.Select_analysis(fnStaticLeakVerified,fnStaticLeakVerifiedFinal,""""FREQUENCY" > 1 """)  #remove peaks that are not verified, i.e. FREQUENCY = 1




