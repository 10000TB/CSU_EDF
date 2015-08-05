/* 
 * Copyright (c) 2015, Colorado State University. Written by Duck Keun Yang 2015-08-02
 * 
 * All rights reserved.
 * 
 * CSU EDF Project
 * 
 * This program periodically checks BigQuery table for any update and store the data into local storage.
 * (further implementation will be storing data into galileo, not file)
 */

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

public class BQDataReceiver {

	private String TableID; // Name of the table in BigQuery
	private String pidListPath; // Path to the file which will store platform_id list and corresponding epoch_time
	private String tmpDataDir; // Path to the directory data will be stored
	private int Interval; // Interval for checking update

	/** variables for internal use */
	private HashMap<String, Double> tmppidlist = new HashMap<String, Double>();


	// [START Constructor]
	/**
	 * Constructor of BQDataReceiver
	 * 
	 * @param TableID			Specify a table to be used
	 * @param Interval			Specify the time in ms to sleep between tasks 
	 * @param pidListFilePath	File path that would be used for maintaining platform id list
	 * @param dataDirectory		directory to store data from bigquery
	 */
	public BQDataReceiver(String TableID, int interval, String pidListFilePath, String dataDirectory){
		this.TableID = TableID;
		this.Interval = interval;
		this.pidListPath = pidListFilePath;
		this.tmpDataDir = dataDirectory;
	}
	// [END Constructor]
	
	// [START Main]
	/**
	 * For testing and showing usage purpose
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) {

		// Change before use! ----------------------------
		/** To change BigQuery ProjectID, modify BigQueryConnecor class. */
		String ProjectID = "csumssummerproject";

		/** Path to the Client Secret File */
		String CLIENTSECRETS_LOCATION = "C:/Users/pinkmaggot/Desktop/Test/OAuth/clients_secrets.json";

		/** Redirect_URI from Google Developers Console */
		String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";

		/** Directory to store user credential files */
		String TMP_DIR = "C:/Users/pinkmaggot/Desktop/Test/OAuth/bq_sample/";

		/** Name of the table in BigQuery */
		String tableID = "earth-outreach:airview.avall_two";

		/** Path to the file which will store platform_id list and corresponding epoch_time */
		String pidlistpath = "C:/Users/pinkmaggot/Desktop/Test/pidList.json";

		/** Path to the directory data will be stored */
		String datadir = "C:/Users/pinkmaggot/Desktop/Test/";

		/** Interval for checking update in ms */
		int interval = 600000;

		/** Host address for galileo server */
		String galileoHostName = "austin.cs.colostate.edu";

		/** Port number for galileo server */
		int galileoPort = 5555;
		// -----------------------------------------------

		// create BigQueryConnector
		try{
			BigQueryConnector bqc = new BigQueryConnector(ProjectID, CLIENTSECRETS_LOCATION, REDIRECT_URI, TMP_DIR);

			// create GalileoConnector
			GalileoConnector gc = new GalileoConnector(galileoHostName, galileoPort);

			// create BQDataReceiver
			BQDataReceiver bqdr = new BQDataReceiver(tableID, interval, pidlistpath, datadir);
			bqdr.start(bqc, gc); // start job
			gc.disconnect();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	// [END Main]

	// [START start]
	/**
	 * Start retrieving data from bigquery table
	 * 
	 * @throws Exception
	 */
	public void start(BigQueryConnector bqc, GalileoConnector gc){
		try {
			initPIDList(bqc, gc); // Initial run
			System.out.println("SYSTEM: Sleep("+Interval+")ms");
			Thread.sleep(Interval);
			autoUpdate(bqc, gc); // Starting a thread...
		} catch (Exception e) {
			System.out.println("Error occured. Failed to start."+e);
			e.printStackTrace();
		}
	}
	// [END start]

	// [START initPIDList]
	/**
	 * Creates an platform_id list and store it into JSON formatted file, and call retreiveData()
	 *
	 * @param BigQueryConnector
	 * @throws Exception
	 */
	private void initPIDList(BigQueryConnector bqc, GalileoConnector gc) throws Exception{
		System.out.println("SYSTEM: Initial platform_id list build started.");
		Path path = Paths.get(pidListPath);
		if (Files.exists(path)) {
			System.out.println("SYSTEM: File("+pidListPath+") already exists.");
			System.out.println("SYSTEM: Loading platform_id list from file...");
			JSONParser parser=new JSONParser();
			JSONArray tmppidjsonarray = new JSONArray();
			tmppidjsonarray = (JSONArray) parser.parse(new FileReader(pidListPath));
			for (Object o : tmppidjsonarray){
				JSONObject platform = (JSONObject) o;
				tmppidlist.put((String) platform.get("platform_id"), (Double) platform.get("lastchecked_epochtime"));
			}
		}else{
			System.out.println("SYSTEM: File("+pidListPath+") does not exists. Creating a new file...");
			List<TableRow> rows = getPlatformIDList(bqc);
			for (TableRow row : rows) {
				for (TableCell field : row.getF()) {
					tmppidlist.put(field.getV().toString(), 0.0);
				}
			}
			writeJSONPIDList(tmppidlist);
			System.out.println("SYSTEM: Initial platform_id list has created.");
		}
		receiveData(bqc, gc);
	}
	// [END initPIDList]

	// [START getPlatformIDList]
	/**
	 * Only executes a single pre-defined query, receive platform_ids from BigQuery table
	 *
	 * @param BigQueryConnector
	 * @return List<TableRow>
	 * @throws Exception	 */
	private List<TableRow> getPlatformIDList(BigQueryConnector bqc) throws Exception{
		System.out.println("SYSTEM: Retrieving platform_ids from Big Query("+TableID+")");
		String querySql = "SELECT platform_id FROM ["+TableID+"] GROUP BY platform_id";
		System.out.println("SYSTEM: Starting query("+querySql+")...");
		return bqc.processQuery(querySql);
	}
	// [END getPlatformIDList]

	// [START writeJSONPIDList]
	/**
	 * Writes HashMap<String,Double> into json formatted file.
	 *
	 * @param HashMap<String, Double>
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked") // Because of JSON Object..
	private void writeJSONPIDList(HashMap<String, Double> hm) throws Exception{
		System.out.println("SYSTEM: Writing platform_id list into json file..");
		FileWriter pidListFile = new FileWriter(pidListPath);
		JSONArray tmp = new JSONArray();
		for(String k : tmppidlist.keySet()){
			JSONObject obj = new JSONObject();
			obj.put("platform_id", k);
			obj.put("lastchecked_epochtime", tmppidlist.get(k));
			tmp.add(obj);
		}
		pidListFile.write(tmp.toJSONString());
		pidListFile.flush();
		pidListFile.close();
	}
	// [END writeJSONPIDList]

	// [START receiveData]
	/**
	 * receives data from BigQuery table and stores it into the file.
	 * Sends a query for each platform_id in platform_id list.
	 *
	 * @param BigQueryConnector
	 * @throws Exception
	 */
	private void receiveData(BigQueryConnector bqc, GalileoConnector gc) throws Exception{
		System.out.println("SYSTEM: Retrieving data from Big Query("+TableID+")");
		@SuppressWarnings("unused") // Because of JSONArray..
		JSONArray tmpjsonarray = new JSONArray();
		for(String k : tmppidlist.keySet()){
			String querySql = "SELECT * FROM ["+TableID+"] WHERE platform_id='"+ k+"' AND datetime>"+tmppidlist.get(k)+" ORDER BY datetime ASC LIMIT 8750";
			System.out.println(querySql);
			long startTime = System.currentTimeMillis();
			long elapsedTime;
			List<TableRow> rows = bqc.processQuery(querySql);
			if(rows==null){
				System.out.println("SYSTEM: The data of platform_id("+k+") is up to date.");
			}else{
				//FileWriter dataWriter = new FileWriter(tmpDataDir+k, true);
				//BufferedWriter bfWriter = new BufferedWriter(dataWriter);
				double max = tmppidlist.get(k);
				for (TableRow row : rows) {
					int i = 0;
					String tmps = "";
					for (TableCell field : row.getF()) {
						if(field.getV().toString().contains("java.lang.Object@")){
							// quick fix for toString() on getV() method..
							// cannot properly convert to string when its null
							tmps+="null,";
						}else{
							tmps+=field.getV().toString()+",";
						}
						i+=1;
						if(i==4){
							double tmp = Double.parseDouble(field.getV().toString());
							if(tmp>=max){
								max = tmp;
							}
						}
					}
					gc.store(GalileoConnector.createBlock(tmps.substring(0, tmps.length()-1)));
					//bfWriter.write(tmps.substring(0, tmps.length()-1)+"\n");
				}
				//bfWriter.close();
				tmppidlist.put(k, max);
				elapsedTime = System.currentTimeMillis() - startTime;
				System.out.println("SYSTEM: "+rows.size()+"rows / "+elapsedTime+"ms. The data of platform_id("+k+") has successfully saved to the file("+tmpDataDir+k+").");
			}
		}
		writeJSONPIDList(tmppidlist);
	}
	// [END receiveData]

	// [START autoUpdate]
	/**
	 * Starts a thread that repeatedly check if there is any update on BigQuery table.
	 *
	 * @param BigQueryConnector
	 * @throws Exception
	 */
	private void autoUpdate(final BigQueryConnector bqc, final GalileoConnector gc){
		Thread t = new Thread(new Runnable() {           
			public void run() { 
				try {
					while(true){
						System.out.println("SYSTEM: Checking any update on Big Query("+TableID+")");
						List<TableRow> rows = getPlatformIDList(bqc);
						boolean dataUpdated = false;
						for (TableRow row : rows) {
							for (TableCell field : row.getF()) {
								if(!tmppidlist.containsKey(field.getV())){
									tmppidlist.put(field.getV().toString(), 0.0);
									dataUpdated = true;
								}
							}
						}
						if(dataUpdated){
							writeJSONPIDList(tmppidlist);
						}
						receiveData(bqc, gc);
						System.out.println("SYSTEM: Sleep("+Interval+"ms)");
						Thread.sleep(Interval);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}
	// [END autoUpdate]
}
