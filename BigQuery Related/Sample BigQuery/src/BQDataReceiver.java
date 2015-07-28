import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.json.simple.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

public class BQDataReceiver {
	/** To change BigQuery ProjectID, modify BigQueryConnecor class. */
	private static final String ProjectID = BigQueryConnector.getProjectID();
	/** Name of the table in BigQuery */
	private static final String TableID = "earth-outreach:airview.avall_two";
	/** Path to the file which will store platform_id list and corresponding epoch_time */
	private static final String pidListPath = "C:/Users/pinkmaggot/Desktop/Test/pidList";
	/** Path to the directory data will be stored */
	private static final String tmpDataDir = "C:/Users/pinkmaggot/Desktop/Test/";
	/** Interval for checking update */
	private static final int Interval = 60000;
	
	// static variables for internal use
	private static HashMap<String, Double> tmppidlist = new HashMap<String, Double>();
	private static JSONArray tmppidjsonarray = new JSONArray();
	private static boolean EXIT_BIT = false;
	
	public static void main(String[] args) {
		Bigquery bigquery; // Google Bigquery API
		try {
			bigquery = BigQueryConnector.createAuthorizedClient();
			initPIDList(bigquery); // Initial run
			System.out.println("SYSTEM: Sleep("+Interval+")");
			Thread.sleep(Interval);
			refreshPlatformIDList(bigquery); // Starting a thread...
		} catch (Exception e) {
			System.out.println("Error occured. "+e);
			e.printStackTrace();
		}
	}
	
	 /**
	   * Creates an platform_id list and store it into JSON formatted file.
	   * 
	   * This also calls retrieveData(Bigquery) to retrieve first set of data from Big Query Table
	   *
	   * @param Bigquery
	   * @throws Exception
	   */
	private static void initPIDList(Bigquery bq) throws Exception{
		System.out.println("SYSTEM: Initial platform_id list build started.");
		Path path = Paths.get(pidListPath);
		if (Files.exists(path)) {
			System.out.println("SYSTEM: File("+pidListPath+") already exists.");
			System.out.println("SYSTEM: Loading platform_id list from file...");
			JSONParser parser=new JSONParser();
			tmppidjsonarray = (JSONArray) parser.parse(new FileReader(pidListPath));
			for (Object o : tmppidjsonarray){
				JSONObject platform = (JSONObject) o;
				tmppidlist.put((String) platform.get("platform_id"), (Double) platform.get("lastchecked_epochtime"));
			}
		}else{
			System.out.println("SYSTEM: File("+pidListPath+") does not exists. Creating a new file...");
			List<TableRow> rows = getPlatformIDList(bq);
			for (TableRow row : rows) {
				for (TableCell field : row.getF()) {
					JSONObject obj = new JSONObject();
					obj.put("platform_id", field.getV());
					obj.put("lastchecked_epochtime", 0.0);
					tmppidjsonarray.add(obj);
					tmppidlist.put(field.getV().toString(), 0.0);
				}
			}
			writeJSONPIDList(tmppidjsonarray);
		}
		System.out.println("SYSTEM: Initial platform_id list has created.");
		retrieveData(bq);
	}
	
	 /**
	   * Sends a query to BigQuery, and return its result as List<TableRow>
	   *
	   * @param Bigquery, String
	   * @return List<TableRow>
	   * @throws Exception
	   */
	private static List<TableRow> processQuery(Bigquery bq, String querysql) throws Exception{
		JobReference jobId = BigQueryConnector.startQuery(bq, ProjectID, querysql);
		Job completedJob = BigQueryConnector.checkQueryResults(bq, ProjectID, jobId);
		GetQueryResultsResponse queryResult = bq.jobs().getQueryResults(ProjectID, completedJob.getJobReference().getJobId()).execute();
		return queryResult.getRows();
	}
	
	 /**
	   * Only executes single query, retrieve platform_ids from BigQuery table
	   *
	   * @param Bigquery
	   * @return List<TableRow>
	   * @throws Exception
	   */
	private static List<TableRow> getPlatformIDList(Bigquery bq) throws Exception{
		System.out.println("SYSTEM: Retrieving platform_ids from Big Query("+TableID+")");
		String querySql = "SELECT platform_id FROM ["+TableID+"] GROUP BY platform_id";
		System.out.println("SYSTEM: Starting query("+querySql+")...");
		return processQuery(bq, querySql);
	}
	
	 /**
	   * Writes JSONArray into the file.
	   *
	   * @param JSONArray
	   * @throws Exception
	   */
	private static void writeJSONPIDList(JSONArray jsonarray) throws Exception{
		System.out.println("SYSTEM: Writing platform_id list into json file..");
		FileWriter pidListFile = new FileWriter(pidListPath);
		pidListFile.write(jsonarray.toJSONString());
		pidListFile.flush();
		pidListFile.close();
	}
	
	 /**
	   * Retrieves data from BigQuery table and stores it into the file.
	   * Sends a query for each platform_id in platform_id list.
	   *
	   * @param Bigquery
	   * @throws Exception
	   */
	private static void retrieveData(Bigquery bq) throws Exception{
		System.out.println("SYSTEM: Retrieving data from Big Query("+TableID+")");
		JSONArray tmpjsonarray = new JSONArray();
		for(String k : tmppidlist.keySet()){
			String querySql = "SELECT * FROM ["+TableID+"] WHERE platform_id='"+k+"' AND epoch_time>"+tmppidlist.get(k)+" ORDER BY epoch_time ASC LIMIT 20000";
			System.out.println(querySql);
			List<TableRow> rows = processQuery(bq, querySql);
			if(rows==null){
				System.out.println("SYSTEM: The data of platform_id("+k+") is up to date.");
			}else{
				FileWriter dataWriter = new FileWriter(tmpDataDir+k, true);
				double max = tmppidlist.get(k);
				for (TableRow row : rows) {
					int i = 0;
					for (TableCell field : row.getF()) {
						dataWriter.write(field.getV().toString()+"\t");
						i+=1;
						if(i==4){
							double tmp = Double.parseDouble(field.getV().toString());
							if(tmp>max){
								max = tmp;
							}
						}
					}
					dataWriter.write("\n");
				}
				dataWriter.close();
				tmppidlist.put(k, max);
				JSONObject obj = new JSONObject();
				obj.put("platform_id", k);
				obj.put("lastchecked_epochtime", max);
				tmpjsonarray.add(obj);
				System.out.println("SYSTEM: The data of platform_id("+k+") has successfully saved to the file("+tmpDataDir+k+").");
			}
		}
		tmppidjsonarray = tmpjsonarray;
		writeJSONPIDList(tmpjsonarray);
	}
	
	/**
	   * Starts a thread that repeatedly check if there is any update on BigQuery table.
	   *
	   * @param Bigquery
	   * @throws Exception
	   */
	private static void refreshPlatformIDList(final Bigquery bq){
		Thread t = new Thread(new Runnable() {           
			public void run() { 
				try {
					while(!EXIT_BIT){
						System.out.println("SYSTEM: Checking any update on Big Query("+TableID+")");
						List<TableRow> rows = getPlatformIDList(bq);
						boolean dataUpdated = false;
						for (TableRow row : rows) {
							for (TableCell field : row.getF()) {
								if(!tmppidlist.containsKey(field.getV())){
									JSONObject obj = new JSONObject();
									obj.put("platform_id", field.getV());
									obj.put("lastchecked_epochtime", 0.0);
									tmppidjsonarray.add(obj);
									tmppidlist.put(field.getV().toString(), 0.0);
									dataUpdated = true;
								}
							}
						}
						if(dataUpdated){
							writeJSONPIDList(tmppidjsonarray);
						}
						retrieveData(bq);
						System.out.println("SYSTEM: Sleep("+Interval+")");
						Thread.sleep(Interval);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}
	
}
