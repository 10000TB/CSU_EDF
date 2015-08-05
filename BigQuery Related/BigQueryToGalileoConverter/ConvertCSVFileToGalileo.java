/* 
 * Copyright (c) 2015, Colorado State University. Written by Duck Keun Yang 2015-08-02
 * 
 * All rights reserved.
 * 
 * CSU EDF Project
 * 
 * This program read a csv-formatted file and send each line to the galileo server
 */

import java.io.FileInputStream;
import java.util.Scanner;

public class ConvertCSVFileToGalileo {

	// [START processFile]
	/**
	 * read each line from the csv file and send it to galileo server
	 * 
	 * @param pathtothefile			path to the csv file
	 * @param galileoconnector		GalileoConnector instance
	 * @throws Exception
	 */
	private static void processFile(String filepath, GalileoConnector gc) throws Exception{
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			inputStream = new FileInputStream(filepath);
			sc = new Scanner(inputStream);
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				if(line.startsWith("platform_id,date,")){
					continue;
				}
				gc.store(GalileoConnector.createBlock(line));
				//System.out.println(line);
			}
			// note that Scanner suppresses exceptions
			if (sc.ioException() != null) {
				throw sc.ioException();
			}
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (sc != null) {
				sc.close();
			}
		}
	}
	// [END processFile]

	// [START Main]
	/**
	 * Based on command line argument, call processFile method to store the data at galileo server
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length!=3){
			System.out.println("Usage: ConvertCSVFileToGalileo [galileo-hostname] [galileo-port-number] [path-to-csv-file]");
			System.exit(0);
		}else{
			try {
				GalileoConnector gc = new GalileoConnector(args[0], Integer.parseInt(args[1]));
				processFile(args[2], gc);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.exit(0);
	}
	// [END Main]
}
