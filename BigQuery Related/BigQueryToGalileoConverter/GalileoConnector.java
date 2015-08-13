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

import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.util.GeoHash;

public class GalileoConnector extends GalileoConnectorInterface {

	// [START Constructor]
	/**
	 * Constructor of GalileoConnector. Use Superclass Constructor.
	 * 
	 * @param serverHostName	Hostname of galileo server
	 * @param serverPort		Portnumber of galileo server
	 * @throws IOException
	 */
	public GalileoConnector(String serverHostName, int serverPort) throws IOException {
		super(serverHostName, serverPort);
	}
	// [END Constructor]

	// [START store]
	/**
	 * stores a block to galileo server. Use Superclass Method
	 * 
	 * @param block		a galileo block to be sent and stored at galileo server
	 * @throws Exception
	 */
	public void store(Block fb) throws Exception {
		super.store(fb);
	}
	// [END store]

	// [START disconnect]
	/**
	 * disconnects from galileo server
	 */
	public void disconnect() {
		super.disconnect();
	}
	// [END disconnect]

	// [START createBlock]
	/**
	 * returns a galileo block from csv formatted EDF data record.
	 * 
	 * @param EDFDataRecord		a single row of csv-formatted EDF data from BigQuery
	 */
	public static Block createBlock(String EDFDataRecord) {
		String[] lines = EDFDataRecord.split("\n");
		long sumdatetime = 0;
		float sumlat = 0f;
		float sumlong = 0f;
		double sumch4 = 0.0;
		for(int i=0; i<lines.length; i++){
			String[] values = lines[0].split(",");
			sumdatetime += reformatDatetime(values[3]);
			sumlat += Float.parseFloat(values[24]);
			sumlong += Float.parseFloat(values[25]);
			sumch4 += Double.parseDouble(values[21]);
		}

		TemporalProperties temporalProperties = new TemporalProperties((sumdatetime/lines.length));
		SpatialProperties spatialProperties = new SpatialProperties((sumlat/lines.length), (sumlong/lines.length));
		
		// TODO Need to be changed
		FeatureSet features = new FeatureSet();
		features.put(new Feature("ch4", sumch4/lines.length));
		
		Metadata metadata = new Metadata();
		metadata.setName(GeoHash.encode( (sumlat/lines.length), (sumlong/lines.length), 7));
		metadata.setTemporalProperties(temporalProperties);
		metadata.setSpatialProperties(spatialProperties);
		metadata.setAttributes(features);

		return new Block(metadata, EDFDataRecord.getBytes());
	}
	// [END createBlock]
	
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