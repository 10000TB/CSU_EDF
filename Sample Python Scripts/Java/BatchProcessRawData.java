import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
/**
 * @author Duck Keun Yang
 * Java version of BatchProcessRawData.py and ProcessRawData_REV.py
 * This is just a direct translation of python scripts. This can be improved later.
 * Added on 2015/06/02: The program has been tested and confirmed as working properly.
 * 
 * # Process raw data files, to:
 * # 1. combine by unique car and day combination
 * # 2. add car number as a field to the processed data...
 * # 3. data QA/QC steps:
 * #   a. remove if >45 mph
 * #   b. check for parameters out of specification for Picarro instrument
 * #   c. bad entries such as car speed: 1.#QNAN00000E+000 or -1.#IND000000E+000 or blank lines **send message to signify what file blank lines are in...
 * #   d. check for lat/long values of 0.0
 * #   e. adjusts x,y location for estimated time delay due to outlet pressure changes
 * # 4. write out as .csv to reduce file size and make ingesting into ArcGIS easier
 * # Written by Dave Theobald, following logic of Jessica Salo from "raw_data_processing.py"
 * 
 * #NEED TO: change myDir, uncomment appropriate s and change date (eg: CFADS2274-201XXXX)
 */
public class BatchProcessRawData {
	static String myDir = "C:/Users/pinkmaggot/Desktop/Test/Example Table 2/"; // NOTE the car# is hardcoded in this line...
	// static String s1 = "CFADS2274-20140"; // is car C10232
	static String s1 = "CFADS2280-20140"; // is car C10241
	// static String s1 = "CFADS2276-201408"; // is car C10293 
	static int gZIP = 0; // 1=gZip off, 0= gZip on
	public static void main(String[] args) {
		String x1 = "";
		boolean bFirst;
		File[] files = new File(myDir).listFiles();
		for (File file : files) {
			// System.out.println(file.getName());
			if(file.getName().startsWith(s1)){
				if (file.getName().substring(0, 18).equals(x1)){
					bFirst = false;
				}else{
					bFirst = true;
				}
				x1 = file.getName().substring(0, 18);
				
				// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
				String xCar = file.getName().substring(0, 9);
				String xDate = file.getName().substring(10, 18);
				boolean theResult = ProcessRawData(xCar, xDate, myDir, file.getName(), bFirst, gZIP);
				System.out.println("Running file: "+file.getName()+", xCar="+xCar+", xDate="+xDate);
				System.out.println("Result: "+theResult);
			}
		}
	}
	
	private static boolean is_double(String in){
		try{
			Double.parseDouble(in);
		}catch(Exception e){
			return false;
		}
		return true;
	}
	
	private static double getMean(List<Double> aList){
		return getMean(aList, 0, aList.size());
	}

	private static double getMean(List<Double> aList, int start, int end){
		DescriptiveStatistics dStat =new DescriptiveStatistics();
		for (int i=start; i < end; ++i) {
			dStat.addValue(aList.get(i));
		}
		return dStat.getMean();
	}

	private static double getStd(List<Double> aList){
		return getStd(aList, 0, aList.size());
	}

	private static double getStd(List<Double> aList, int start, int end){
		DescriptiveStatistics dStat=new DescriptiveStatistics();
		for (int i=start; i < end; ++i) {
			dStat.addValue(aList.get(i));
		}
		return dStat.getStandardDeviation();
	}
	
	// # NEED TO: change xOutDir
	private static boolean ProcessRawData(String xCar, String xDate, String xDir, String xFilename, boolean bFirst, int gZIP) {
		try{
			// print "Processing Raw Data on: " + xCar +", " + xDate
			// assumes Python started in Terminal located in root directory: 
			//  "/Users/davidtheobald/Documents/projects/EDF_MethaneMapping/RawData/C10241/"
			// xOutDir = "C:/Source/Mapping/GSV/C10241/"
			String xOutDir = "C:/Users/pinkmaggot/Desktop/Test/testout/";
			// car C10232 = CFADS2274;
			//// xCar = "C10232"
			//// xDate2 = "20121112"
			// gZIP = 0    #1 = true, 0 equals false

			double xMaxCarSpeed = 45.0 / 2.23694; // assumes 45 mph as max, conver to meters per second 
			// data quality thresholds
			// xCavPL = 140.0 - 0.1; xCavPH = 140.0 + 0.1; xCavTL = 45.0 - 0.005; xCavTH = 45.0 + 0.005; xWarmBoxTL = 45.0 - 0.005; xWarmBoxTH = 45.0 + 0.005  # from Kathy McCain
			double xCavPL = 140.0 - 1; double xCavPH = 140.0 + 1; double xCavTL = 45.0 - 1; double xCavTH = 45.0 + 1; double xWarmBoxTL = 45.0 - 1; double xWarmBoxTH = 45.0 + 1;  // quality thresholds (relaxed)

			double xX2 = 0.0, xX1 = 0.0, xX0 = 0.0; int xOutletPressureLow = 0, xOutletPressureHigh = 0; double xOutletPressureHighValue = 0.0, xDelay = 0.0;
			// set up outlet-pressure delay parameters, specific to each car/instrument
			if(xCar.equals("CFADS2274")){
				// is car C10232
				xX2 = 0.00000006; xX1 = 0.0031; xX0 = 46.711; xOutletPressureLow = 19000; xOutletPressureHigh = 27000; xOutletPressureHighValue = 5.5; xDelay = 1.5;
			}else if(xCar.equals("CFADS2280")){
				// is car C10241
				xX2 = 0.00000004; xX1 = 0.0036; xX0 = 82.483; xOutletPressureLow = 32000; xOutletPressureHigh = 39000; xOutletPressureHighValue = 6.3; xDelay = 1.5;
			}else if(xCar.equals("CFADS2276")){
				// is car C10293
				xX2 = 0.00000008; xX1 = 0.0044; xX0 = 61.386; xOutletPressureLow = 19000; xOutletPressureHigh = 24000; xOutletPressureHighValue = 4.8; xDelay = 1.5;
			}

			// set header for fields
			//                0    1    2                    3                   4           5          6            7           8              9          10      11         12          13      14          15          16              17  18      19  20      21  22          23           24      25          26          27             28             29     30     31            32          33        34     35
			String sHeader = "DATE,TIME,FRAC_DAYS_SINCE_JAN1,FRAC_HRS_SINCE_JAN1,JULIAN_DAYS,EPOCH_TIME,ALARM_STATUS,INST_STATUS,CavityPressure,CavityTemp,DasTemp,EtalonTemp,WarmBoxTemp,species,MPVPosition,OutletValve,solenoid_valves,CO2,CO2_dry,CH4,CH4_dry,H2O,GPS_ABS_LAT,GPS_ABS_LONG,GPS_FIT,WS_WIND_LON,WS_WIND_LAT,WS_COS_HEADING,WS_SIN_HEADING,WIND_N,WIND_E,WIND_DIR_SDEV,WS_ROTATION,CAR_SPEED,CAR_ID,WKT\n";
			String sHisHeader = "CAR,DATE,CavPressMean,CavPressSTD,CavTempMean,CavTempSTD,WarmBTMean,WarmBTSTD,OutletPMean,OutletPSTD,CH4Mean,CH4STD,CarVelocityMean,CarVelocitySTD,WIND_NMean,WIND_EMean\n";
			// compile summary histogram data
			List<Double> x8 = new ArrayList<Double>();
			List<Double> x9 = new ArrayList<Double>();
			List<Double> x12 = new ArrayList<Double>();
			List<Double> x15 = new ArrayList<Double>();
			List<Double> x19 = new ArrayList<Double>();
			List<Double> x22 = new ArrayList<Double>();
			List<Double> x23 = new ArrayList<Double>();
			List<Double> x30 = new ArrayList<Double>();
			List<Double> x31 = new ArrayList<Double>();
			List<Double> x33 = new ArrayList<Double>();
			
			
			// get all the files in the subdirectory
			// xDir2 = xDir + xCar // +"/"+xDate+"/"
			String f = xDir+"/"+xFilename;
			GZIPInputStream in = new GZIPInputStream(new FileInputStream(f));
			Reader decoder = new InputStreamReader(in);
			BufferedReader br;
			if (gZIP == 0){
				br = new BufferedReader(decoder);
			}else{
				br = new BufferedReader(new FileReader(f));
			}
			// process    
	        // if first time on this car/date, then write header out
	        String fnOut = xOutDir + xCar + "_" + xDate + "_dat.csv"; // set CSV output for raw data
	        String fnLog = xOutDir + xCar + "_" + xDate + "_log.csv"; // output for logfile
	        String fnHis = xOutDir + xCar + "_" + xDate + "_his.csv"; // set histogram output for raw data
	        
	        BufferedWriter logWriter;
			BufferedWriter hisWriter;
			BufferedWriter fOutWriter;
	        
	        if(bFirst){
	        	fOutWriter = new BufferedWriter(new FileWriter(fnOut, true));
	        	fOutWriter.write(sHeader);
	        	hisWriter = new BufferedWriter(new FileWriter(fnHis, true));
	        	logWriter = new BufferedWriter(new FileWriter(fnLog, true));
	        	hisWriter.write(sHisHeader);
	        	System.out.println("fnLog: "+fnOut);
	        }else{
	        	fOutWriter = new BufferedWriter(new FileWriter(fnOut, true));
	        	hisWriter = new BufferedWriter(new FileWriter(fnHis, true));
	        	logWriter = new BufferedWriter(new FileWriter(fnLog, true));
	        }
	        
	        // read all lines
	        int xCntObs = 0;
	        int xCntGoodValues = 0;
	        int iDelay = 0;
	        String line = "";
	        boolean bGood;
	        while ((line = br.readLine()) != null) {
				bGood = true;
				String s1 ="";
				for(int i=0; i<34; i++){
					int xStart = i*26;
					int xEnd = xStart+26;
					String s2 = line.substring(xStart, xEnd).replace(" ", "");
					s1 += s2 + ",";
					if (i > 1){
						if (!is_double(s2)){
							bGood = false;
							// not a number
						}
					}
				}
				if(bGood){
					String lstS[] = s1.split(",");
					// get raw values to summarize over each file, including GPS locations (22 = lat, 23 = long)
					x8.add(Double.parseDouble(lstS[8])); x9.add(Double.parseDouble(lstS[9])); x12.add(Double.parseDouble(lstS[12]));
					x15.add(Double.parseDouble(lstS[15])); x19.add(Double.parseDouble(lstS[19])); x22.add(Double.parseDouble(lstS[22]));
					x23.add(Double.parseDouble(lstS[23])); x30.add(Double.parseDouble(lstS[30])); x31.add(Double.parseDouble(lstS[31]));
					x33.add(Double.parseDouble(lstS[33]));
					
					if (Float.parseFloat(lstS[19]) < 1.5){
						logWriter.write("CH4 value less than 1.5: "+ lstS[19] + "\n");
						continue;
					}
					if (Float.parseFloat(lstS[33]) > xMaxCarSpeed){
						logWriter.write("Car speed of " + lstS[33] + " exceeds max threshold of: " + xMaxCarSpeed + "\n");
						continue;
					}
					// RSSI applies only to CSU vehicle
		            // if float(lstS[28]) < 45.0:
		            // fLog.write("RSSI value less than 45: " + lstS[28] + "\n")
		            // continue
					if (Float.parseFloat(lstS[8]) < xCavPL){
						logWriter.write("Cavity Pressure " + lstS[8] + " outside of range: " + xCavPL + "\n");
						continue;
					}
					if (Float.parseFloat(lstS[8]) > xCavPH){
						logWriter.write("Cavity Pressure " + lstS[8] + " outside of range: " + xCavPH + "\n");
						continue;
					}
					if (Float.parseFloat(lstS[9]) < xCavTL){
						logWriter.write("Cavity Temperature " + lstS[9] + " outside of range: " + xCavTL + "\n");
						continue;
					}
					if (Float.parseFloat(lstS[9]) > xCavTH){
						logWriter.write("Cavity Temperature " + lstS[9] + " outside of range: " + xCavTH + "\n");
						continue;
					}
					if (Float.parseFloat(lstS[12]) < xWarmBoxTL){
						logWriter.write("Warm Box Temperature " + lstS[12] + " outside of range: " + xWarmBoxTL + "\n");
						continue;
					}
					if (Float.parseFloat(lstS[12]) > xWarmBoxTH){
						logWriter.write("Warm Box Temperature " + lstS[12] + " outside of range: " + xWarmBoxTH + "\n");
						continue;
					}
					
					// test for outlet pressure and adjust delay time/location based on function
					double xOutletPressure = Double.parseDouble(lstS[15]);
					Double xTimeDelay=new Double(0.0);
					if (xOutletPressure < xOutletPressureLow){
						logWriter.write("Outlet Pressure " + xOutletPressure + " below minimum value: " + xOutletPressureLow + "\n");
						xTimeDelay = 0.0; // ???
						continue;
					}else if(xOutletPressure > xOutletPressureHigh){
						xTimeDelay = xOutletPressureHighValue - xDelay;
					}else{
						xTimeDelay = (xX2 * xOutletPressure * xOutletPressure) + (xX1 * xOutletPressure) + xX0 - xDelay;
					}
					
					// adjust the x,y location based on time delay. Assume 2 observations per second. Need to store the x,y locations in an array to access back in time.
					iDelay = xCntGoodValues - (xTimeDelay.intValue()*2);
					if(iDelay < 0){
						iDelay = 0;
					}
					s1 += xCar + ",POINT("+x23.get(iDelay)+" "+x22.get(iDelay)+")\n";
					fOutWriter.write(s1);
					xCntGoodValues +=1;
				}
				xCntObs+=1;
				// print str(xCntObs) + ", " + str(xCntGoodValues) + ", " + str(iDelay)
			    // $sOut = "From " + str(f) + " Read " + str(xCntObs) + " lines in, wrote "+ str(xCntGoodValues) + " lines out\n"
			}
			String sOut = gZIP + ","+f+","+xCntObs+","+xCntGoodValues+"\n";
			logWriter.write(sOut);
			
			br.close();
			decoder.close();
			in.close();
			logWriter.close();
			fOutWriter.close();
			
			String sHis = xCar+","+xDate+","+getMean(x8)+","+getStd(x8)+","+getMean(x9)+","+getStd(x9)+","+getMean(x12)+","+getStd(x12)+","+getMean(x15)+","+getStd(x15)+","+getMean(x19)+","+getStd(x19)+","+getMean(x33)+","+getStd(x33)+","+getMean(x30)+","+getMean(x31)+"\n";
			hisWriter.write(sHis);
			hisWriter.close();
			
			// print xCar + "," + xDate + "," + xFilename + "," + str(xCntObs) + "," + str(xCntGoodValues) + "," + str(gZIP)
			System.out.println(xCar + "\t" + xDate + "\t" + xFilename + "\t" + xCntObs + "\t" + xCntGoodValues + "\t" + gZIP);
			
			return true;
		}catch(Exception e){
			System.out.print("Error in IdentifyPeaks: "+e);
			return false;
		}
	}
}
