/*
 * Copyright (c) 2013, Colorado State University. Modified by Duck Keun Yang for CSU EDF Project 2015-08-02
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *    
 * This software is provided by the copyright holders and contributors "as is" and
 * any express or implied warranties, including, but not limited to, the implied
 * warranties of merchantability and fitness for a particular purpose are
 * disclaimed. In no event shall the copyright holder or contributors be liable for
 * any direct, indirect, incidental, special, exemplary, or consequential damages
 * (including, but not limited to, procurement of substitute goods or services;
 * loss of use, data, or profits; or business interruption) however caused and on
 * any theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of this
 * software, even if advised of the possibility of such damage.
 */

import java.io.IOException;
import galileo.client.EventPublisher;
import galileo.comm.StorageRequest;
import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.net.ClientMessageRouter;
import galileo.net.NetworkDestination;

public class GalileoConnector {

	private ClientMessageRouter messageRouter;
	private EventPublisher publisher;
	private NetworkDestination server;

	// [START Constructor]
	/**
	 * Constructor of GalileoConnector
	 * 
	 * @param serverHostName	Hostname of galileo server
	 * @param serverPort		Portnumber of galileo server
	 * @throws IOException
	 */
	public GalileoConnector(String serverHostName, int serverPort) throws IOException {
		messageRouter = new ClientMessageRouter();
		publisher = new EventPublisher(messageRouter);
		server = new NetworkDestination(serverHostName, serverPort);
	}
	// [END Constructor]

	// [START createBlock]
	/**
	 * returns a galileo block from csv formatted EDF data record.
	 * 
	 * @param EDFDataRecord		a single row of csv-formatted EDF data from BigQuery
	 */
	public static Block createBlock(String EDFDataRecord) {
		String[] values = EDFDataRecord.split(",");

		TemporalProperties temporalProperties = new TemporalProperties(reformatDatetime(values[3]));
		SpatialProperties spatialProperties = new SpatialProperties(Float.parseFloat(values[24]), Float.parseFloat(values[25]));

		// TODO Need to be changed
		FeatureSet features = new FeatureSet();
		features.put(new Feature("platform_id", values[0]));
		features.put(new Feature("warm_box_temp", Double.parseDouble(values[14])));
		features.put(new Feature("ch4", Double.parseDouble(values[21])));
		features.put(new Feature("postal_code", values[37]));

		Metadata metadata = new Metadata();
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);

		return new Block(metadata, EDFDataRecord.getBytes());
	}
	// [END createBlock]

	// [START store]
	/**
	 * stores a block to galileo server
	 * 
	 * @param block		a galileo block to be sent and stored at galileo server
	 * @throws Exception
	 */
	public void store(Block fb) throws Exception {
		StorageRequest store = new StorageRequest(fb);
		publisher.publish(server, store);
	}
	// [END store]

	// [START disconnect]
	/**
	 * disconnects from galileo server
	 */
	public void disconnect() {
		messageRouter.shutdown();
	}
	// [END disconnect]

	// [START reformatDatetime]
	/**
	 * reformat epoch_time from BigQuery table into typical UNIX epoch time
	 * FROM: 1.43699552323E9 TO: 143699552323
	 */
	private static long reformatDatetime(String data){
		String tmp = data.replace(".", "").replace("E9", "");
		while(tmp.length()<13){
			tmp+="0";
		}
		return Long.parseLong(tmp); 
	}
	// [END reformatDatetime]
}