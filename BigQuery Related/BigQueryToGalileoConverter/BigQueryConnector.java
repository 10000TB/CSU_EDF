/*
 * Copyright (c) 2012 Google Inc. Modified by Duck Keun Yang for CSU EDF Project 2015-08-02
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

/**
 * Example of authorizing with BigQuery and reading from a public dataset.
 */
public class BigQueryConnector {

	// [START credentials]
	/////////////////////////
	// CHANGE ME!
	// USER GENERATED VALUES: you must fill in values specific to your application.
	//
	// Visit https://cloud.google.com/console to create a Project and generate an
	// OAuth 2.0 Client ID and Secret.
	// See the README for more info.
	// Then, add the Project ID below, and point the CLIENTSECRETS_LOCATION file
	// to the file you downloaded.
	/////////////////////////
	private String PROJECT_ID; // "csumssummerproject";
	private String CLIENTSECRETS_LOCATION; // "C:/Users/pinkmaggot/Desktop/Test/OAuth/clients_secrets.json";

	private GoogleClientSecrets clientSecrets;

	// Static variables for API scope, callback URI, and HTTP/JSON functions
	private final List<String> SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);
	@SuppressWarnings("unused")
	private String REDIRECT_URI; // "urn:ietf:wg:oauth:2.0:oob";

	/** Global instances of HTTP transport and JSON factory objects. */
	private final HttpTransport TRANSPORT = new NetHttpTransport();
	private final JsonFactory JSON_FACTORY = new JacksonFactory();

	@SuppressWarnings("unused")
	private GoogleAuthorizationCodeFlow flow = null;
	private Bigquery bigquery;

	/** Directory to store user credentials. */
	private java.io.File DATA_STORE_DIR; // new java.io.File("C:/Users/pinkmaggot/Desktop/Test/OAuth/bq_sample/");

	/**
	 * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
	 * globally shared instance across your application.
	 */
	private static FileDataStoreFactory dataStoreFactory;

	// [START Constructor]
	/**
	 * Constructor of BigQueryConnector
	 * 
	 * @param ProjectID // Project ID for Google Cloud Platform
	 * @param ClientSecretPath // Path to the secret.json file
	 * @param RedirectURI // Redirect URI from API&Credentials of Google Developers Console
	 * @param UserDataPath // directory to stroe user credential files
	 */
	public BigQueryConnector(String ProjectID, String ClientSecretPath, String RedirectURI, String UserDataDir){
		this.PROJECT_ID = ProjectID;
		this.CLIENTSECRETS_LOCATION = ClientSecretPath;
		this.REDIRECT_URI = RedirectURI;
		this.DATA_STORE_DIR = new java.io.File(UserDataDir);
		try {
			clientSecrets = loadClientSecrets();
			bigquery = createAuthorizedClient();
		} catch (IOException e) {
			System.out.println("Error in Constructor, Could not create BigQueryConnector instance.");
			e.printStackTrace();
		}
	}
	// [END Constructor]

	// [START Main]
	/**
	 * For testing and showing usage purpose
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String projid = "csumssummerproject";
		String secretlocation = "C:/Users/pinkmaggot/Desktop/Test/OAuth/clients_secrets.json";
		String redirecturi = "urn:ietf:wg:oauth:2.0:oob";
		String datapath = "C:/Users/pinkmaggot/Desktop/Test/OAuth/bq_sample/";
		BigQueryConnector bqc = new BigQueryConnector(projid, secretlocation, redirecturi, datapath);
		
		// Print out available datasets in the "publicdata" project to the console
		bqc.listDatasets();

		// Start a Query Job
		String querySql = "SELECT * FROM [earth-outreach:airview.avall_two] WHERE platform_id='10241' LIMIT 10";
		System.out.println("\n\nquery1:\n------------\n"+querySql);
		JobReference jobId = bqc.startQuery(querySql);

		// Poll for Query Results, return result output
		Job completedJob = bqc.checkQueryResults(jobId);

		// Return and display the results of the Query Job
		bqc.displayQueryResults(completedJob);
		
		// example using processQuery method.
		String querySql2 = "SELECT * FROM [earth-outreach:airview.avall_two] WHERE platform_id='10241' LIMIT 10";
		System.out.println("\n\nquery2:\n------------\n"+querySql2);
		List<TableRow> result; 
		try{
			result = bqc.processQuery(querySql2);
			for (TableRow row : result) {
				for (TableCell field : row.getF()) {
					System.out.printf("%-20s", field.getV());
				}
				System.out.println();
			}
		}catch(Exception e){
			System.out.println("Error occured when processing query.");
			e.printStackTrace();
		}
	}	 
	// [END Main]

	// [START credentials]
	/** Authorizes the installed application to access user's protected data. */
	private Credential authorize() throws IOException {
		dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
		// set up authorization code flow
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES).setDataStoreFactory(
						dataStoreFactory).build();
		// authorize
		return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
	}
	// [END credentials]

	/**
	 * Creates an authorized BigQuery client service using the OAuth 2.0 protocol
	 *
	 * This method first creates a BigQuery authorization URL, then prompts the
	 * user to visit this URL in a web browser to authorize access. The
	 * application will wait for the user to paste the resulting authorization
	 * code at the command line prompt.
	 *
	 * @return an authorized BigQuery client
	 * @throws IOException
	 */
	private Bigquery createAuthorizedClient() throws IOException {
		Credential credential = authorize();
		return new Bigquery(TRANSPORT, JSON_FACTORY, credential);
	}

	/**
	 * Display all BigQuery datasets associated with a project
	 *
	 * @throws IOException
	 */
	public void listDatasets()
			throws IOException {
		Datasets.List datasetRequest = bigquery.datasets().list(PROJECT_ID);
		DatasetList datasetList = datasetRequest.execute();
		if (datasetList.getDatasets() != null) {
			List<DatasetList.Datasets> datasets = datasetList.getDatasets();
			System.out.println("Available datasets\n----------------");
			System.out.println(datasets.toString());
			for (DatasetList.Datasets dataset : datasets) {
				System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
			}
		}
	}

	// [START start_query]
	/**
	 * Creates a Query Job for a particular query on a dataset
	 *
	 * @param querySql  the actual query string
	 * @return a reference to the inserted query job
	 * @throws IOException
	 */
	public JobReference startQuery(String querySql) throws IOException {
		//System.out.format("\nInserting Query Job: %s\n", querySql);

		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		config.setQuery(queryConfig);

		job.setConfiguration(config);
		queryConfig.setQuery(querySql);

		Insert insert = bigquery.jobs().insert(PROJECT_ID, job);
		insert.setProjectId(PROJECT_ID);
		JobReference jobId = insert.execute().getJobReference();

		//System.out.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

		return jobId;
	}

	/**
	 * Polls the status of a BigQuery job, returns Job reference if "Done"
	 *
	 * @param jobId     a reference to an inserted query Job
	 * @return a reference to the completed Job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public Job checkQueryResults(JobReference jobId)
			throws IOException, InterruptedException {
		// Variables to keep track of total query time
		//long startTime = System.currentTimeMillis();
		//long elapsedTime;

		while (true) {
			Job pollJob = bigquery.jobs().get(PROJECT_ID, jobId.getJobId()).execute();
			//elapsedTime = System.currentTimeMillis() - startTime;
			//System.out.format("Job status (%dms) %s: %s\n", elapsedTime, jobId.getJobId(), pollJob.getStatus().getState());
			if (pollJob.getStatus().getState().equals("DONE")) {
				return pollJob;
			}
			// Pause execution for one second before polling job status again, to
			// reduce unnecessary calls to the BigQUery API and lower overall
			// application bandwidth.
			Thread.sleep(100);
		}
	}
	// [END start_query]

	// [START processQuery]
	/**
	 * Sends a query to BigQuery, and return its result as List<TableRow>
	 *
	 * @param String
	 * @return List<TableRow>
	 * @throws Exception
	 */
	public List<TableRow> processQuery(String querysql) throws Exception{
		JobReference jobId = startQuery(querysql);
		Job completedJob = checkQueryResults(jobId);
		GetQueryResultsResponse queryResult = bigquery.jobs().getQueryResults(PROJECT_ID, completedJob.getJobReference().getJobId()).execute();
		return queryResult.getRows();
	}
	// [END processQuery]


	// [START display_result]
	/**
	 * Makes an API call to the BigQuery API
	 *
	 * @param completedJob to the completed Job
	 * @throws IOException
	 */
	public void displayQueryResults(Job completedJob) throws IOException {
		GetQueryResultsResponse queryResult = bigquery.jobs()
				.getQueryResults(
						PROJECT_ID, completedJob
						.getJobReference()
						.getJobId()
						).execute();
		List<TableRow> rows = queryResult.getRows();
		System.out.print("\nQuery Results:\n------------\n");
		for (TableRow row : rows) {
			for (TableCell field : row.getF()) {
				System.out.printf("%-20s", field.getV());
			}
			System.out.println();
		}
	}
	// [END display_result]

	/**
	 * Helper to load client ID/Secret from file.
	 *
	 * @return a GoogleClientSecrets object based on a clientsecrets.json
	 */
	private GoogleClientSecrets loadClientSecrets() {
		try {
			InputStream inputStream = new FileInputStream(CLIENTSECRETS_LOCATION);
			Reader reader =
					new InputStreamReader(inputStream);
			GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(new JacksonFactory(),
					reader);
			return clientSecrets;
		} catch (Exception e) {
			System.out.println("Could not load client secrets file " + CLIENTSECRETS_LOCATION);
			e.printStackTrace();
		}
		return null;
	}
}
