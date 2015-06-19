import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
/**
 * @author Duck Keun Yang
 * Java version of BatchIdentifyPeaks.py and IdentifyPeaks.py
 * This is just a direct translation of python scripts. This can be improved later.
 * 
 * Assumes that the IdentifyPeaks subroutine has been loaded
 * NEED TO: modify myDir, uncomment appropriate s's and change date, and change s in "if file.startswith(sX)
 */
public class BatchIdentifyPeaks {

	static String myDir = "C:/Data/FortCollins/NewDataProcessing_140804/";
	static 
	//String s1 = "CFADS2274_2014";    // is car C10232
	String s2 = "CFADS2280_20140";    // is car C10241
	//String s3 = "CFADS2276_201";    // is car C10293

	public static void main(String[] args) {
		File[] files = new File("myDir").listFiles();
		for (File file : files) {
			// System.out.println(file.getName());
			if(file.getName().startsWith(s2)){
				// System.out.println(file.getName().substring(19, 23));
				if(file.getName().substring(19, 22).equals("dat")){
					// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>");
					String xCar = file.getName().substring(0, 9);
					String xDate = file.getName().substring(10, 18);
					boolean theResult = IdentifyPeaks(xCar, xDate, myDir, file.getName());
					System.out.println("Running file: "+file.getName()+", xCar="+xCar+", xDate="+xDate);
					System.out.println("Result: "+theResult);
				}
			}
		}
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

	private static boolean IdentifyPeaks(String xCar, String xDate,	String xDir, String xFilename) {
		// "/Users/davidtheobald/Documents/projects/EDF_MethaneMapping/RawData/"
		// xDir = "/Users/davidtheobald/Documents/projects/EDF_MethaneMapping/RawData/"
		// xCar = "C10232"
		// xDate = "20121112"
		try{
			double xABThreshold = 0.2; // above baseline threshold above the mean value
			double xDistThreshold = 20.0; // find the maximum CH4 reading of observations within street segments of this grouping distance in meters
			int xSDF = 1; // multiplier times standard deviation for Doubleing baseline added to mean
			int xB = 240; // the number of records that constitutes the Doubleing baseline time -- 7200 = 1 hour (assuming average of 0.5 seconds per record)
			// String xCity = "FC"; double xLonMin = -105.171; double xLatMin = 40.465; double xLonMax = -104.978; double xLatMax = 40.653; // city abbreviation and bounding box in geographic coordinates
			String xCity = "US"; double xLonMin = -126.0; double xLatMin = 24.5; double xLonMax = -65.0; double xLatMax = 50.0; // city abbreviation and bounding box in geographic coordinates

			// String fn = xDir + xCar + "_" + xDate + ".csv"      // set raw text file to read in
			String fn = xDir+"/"+xFilename; // set raw text file to read in
			String fnOut = xDir + "Peaks" + "_" + xCar + "_" + xDate + ".csv"; // set CSV format output for observed peaks for a given car, day, city
			String fnLog = xDir + "Peaks" + "_" + xCar + "_" + xDate + ".log"; // set CSV output for observed peaks for a given car, day, city
			String fnPoints = xDir + "Location_"+xCar+".csv"; // set CSV output for x,y locations of cars by days
			BufferedWriter logWriter = new BufferedWriter(new FileWriter(fnLog, true));
			BufferedWriter pointsWriter = new BufferedWriter(new FileWriter(fnPoints, true));

			// field column indices for various variables
			int fFracDays = 2; int fFracHours = 3; int fEpochTime = 5; int fAlarm = 6; int fCavP = 8; int fCavT = 9; int fWarmBoxT = 12; int fCH4 = 19; int fLat = 22; int fLon = 23;

			// read data in from text file and extract desired fields into a list, padding with 5 minute and hourly average
			List<Double> aFracHours = new ArrayList<Double>();
			List<Double> aCH4 = new ArrayList<Double>();
			List<Double> aLat = new ArrayList<Double>();
			List<Double> aLon = new ArrayList<Double>();
			List<Double> aMean = new ArrayList<Double>();
			List<Double> aEpochTime = new ArrayList<Double>();

			int count = 0;

			BufferedReader br = new BufferedReader(new FileReader(fn));
			String cvsSplitBy = ","; // use comma as separator
			String line = "";
			String fieldName1 = ""; String fieldName2=""; String fieldName3=""; String fieldName4="";

			while ((line = br.readLine()) != null) {
				String[] col = line.split(cvsSplitBy);
				if(count==0){
					System.out.println(line);
					fieldName1 = col[fFracHours]; fieldName2 = col[fCH4]; fieldName3 = col[fLat]; fieldName4 = col[fLon];
				}else{
					aFracHours.add(Double.parseDouble(col[fFracHours]));
					aCH4.add(Double.parseDouble(col[fCH4]));
					aLat.add(Double.parseDouble(col[fLat]));
					aLon.add(Double.parseDouble(col[fLon]));
					aMean.add(0.0);
					aEpochTime.add(Double.parseDouble(col[fEpochTime]));
				}
				count++;
			}
			// print "Number of points outside the specified city's bounding box: " + str(cntOutside)
			System.out.println("Number of observations processed: " + count);

			// aFracHours = aFracHours; aCH4 = aCH4; aLat = aLat; aLon = aLon; aMean = aMean; aEpochTime = aEpochTime

			// find observations with CH4 greater than mean+t -- Above Baseline
			List<Integer> lstCH4_AB = new ArrayList<Integer>();
			double xCH4Mean = getMean(aCH4, 0, xB); // initial floating baseline
			double xCH4SD = getStd(aCH4, 0, xB);
			if(xCH4SD < (0.1 * xCH4Mean)){
				xCH4SD = (0.1 * xCH4Mean); // ensure that SD is at least ~0.2
			}

			double xLatMean = getMean(aLat);
			double xLonMean = getMean(aLon);

			logWriter.write("Day CH4_mean = " + getMean(aCH4) + ", Day CH4_SD = " + getStd(aCH4) + "\n");
			logWriter.write("Center lon/lat = " + xLonMean + ", " + xLatMean + "\n");
			// fPoints.write(str(xCar) + "," + str(xDate) + "," + str(xLonMean) + "," + str(xLatMean) +"," + str(count) + "," + str(numpy.mean(aCH4)) + "," + str(xCH4SD) + ",POINT(" + str(xLonMean) + " " + str(xLatMean) + ")\n")

			// generate list of the index for observations that were above the threshold
			double xThreshold = 0.0;
			for(int i=0; i<count-2; i++){
				// find floating baseline mean
				if( i > xB){
					xCH4Mean = getMean(aCH4, i-xB, i);
					xCH4SD = getStd(aCH4, i=xB, i);
					if(xCH4SD < (0.1 * xCH4Mean)){
						xCH4SD = (0.1 * xCH4Mean); // ensure that SD is at least ~0.2
					}
				}
				xThreshold = xCH4Mean + (xCH4SD * xSDF);
				if( aCH4.get(i) > xThreshold){
					lstCH4_AB.add(i);
					aMean.set(i, xThreshold); // #insert mean + SD as upper quartile CH4 value into the array to later retreive into the peak calculation
				}
			}

			// now group the above baseline threshold observations into groups based on distance threshold
			List<String> lstCH4_ABP = new ArrayList<String>();
			double xDistPeak = 0.0;
			double xCH4Peak = 0.0;
			double xTime = 0.0;
			int cntPeak = 0;
			int cnt = 0;
			String sID = "";
			String sPeriod5Min = "";
			double xLon1 = 0.0;
			double xLat1 = 0.0;
			double xDist = 0.0;
			for(int i=0; i<lstCH4_AB.size(); i++){
				if(cnt==0){
					xLon1 = aLon.get(i);
					xLat1 = aLat.get(i);
				}else{
					// calculate distance between points
					xDist = Math.sqrt(Math.pow((aLon.get(i)-xLon1)*(111319.9*Math.cos(Math.toRadians(aLat.get(i)))),2)+Math.pow(((aLat.get(i)-xLat1)*111319.9), 2));
					xDistPeak += xDist;
					xCH4Peak += (xDist*(aCH4.get(i)-aMean.get(i)));
					xLon1 = aLon.get(i);
					xLat1 = aLat.get(i);
					if(sID.equals("")){
						xTime = aFracHours.get(i);
						sID = xCar+"_"+xDate+"_"+xTime;
						sPeriod5Min = Integer.toString((aEpochTime.get(i).intValue()-1350000000) / (60*5));
					}
					if(xDist > xDistThreshold){ // initial start of a observed peak
						// calc maximum value for a peak
						/* 
						 * if (xDistPeak > 160.0):
						 * 		xType=0
						 * else:
						 * 		xType=1
						 * 
						 * for j in range(cntstart, i):
						 * 		lstpeakArea.append(xType)
						 */
						cntPeak+=1;
						xTime = aFracHours.get(i);
						xDistPeak = 0.0;
						xCH4Peak = 0.0;
						sID = xCar+"_"+xDate+"_"+xTime;
						sPeriod5Min = Integer.toString((aEpochTime.get(i).intValue()-1350000000) / (60*5));
						// print str(i)+", "+str(xDist)+", "+str(cntPeak)+", "+str(xDistPeak)
					}
					// lstCH4_ABP.append([sID, xTime, aFracHours[i], aCH4[i], aLat[i], aLon[i], aMean[i], xDistPeak, xCH4Peak)
					lstCH4_ABP.add(sID+","+xTime+","+aFracHours.get(i)+","+aCH4.get(i)+",POINT("+aLon.get(i)+" "+aLat.get(i)+")"+aLon.get(i)+","+aLat.get(i)+","+aMean.get(i)+","+xDistPeak+","+xCH4Peak+","+sPeriod5Min);
				}
				cnt+=1;
			}

			logWriter.write("Number of peaks found: "+cntPeak+"\n");
			pointsWriter.write(xCar+","+xDate+","+xLonMean+","+xLatMean+","+count+","+getMean(aCH4)+","+xCH4SD+","+cntPeak+",POINT("+xLonMean+" "+xLatMean+")\n");
			System.out.println(xCar+"\t"+xDate+"\t"+xFilename+"\t"+count+"\t"+cntPeak);
			// calculate attribute for the area under the curve -- PPM

			// write out the observed peaks to a csv to be read into a GIS
			BufferedWriter fOutWriter = new BufferedWriter(new FileWriter(fnOut, true));
			fOutWriter.write("PEAK_NUM_FRACHRSTART,"+fieldName1+","+fieldName2+",WKT"+",LON,LAT,CH4_BASELINE,PEAK_DIST_M,PEAK_CH4,PERIOD5MIN\n");
			for(int i=0; i<lstCH4_ABP.size(); i++){
				// System.out.println(lstCH4_ABP.get(i));
				fOutWriter.write(lstCH4_ABP.get(i));
			}
			br.close();
			fOutWriter.close();
			logWriter.close();
			pointsWriter.close();

			return true;

		}catch(Exception e){
			System.out.print("Error in IdentifyPeaks: "+e);
			return false;
		}
	}
}
