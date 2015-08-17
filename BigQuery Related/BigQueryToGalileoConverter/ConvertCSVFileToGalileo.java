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
			
			double max=0.0; 
            double maxplus10sec=0.0;  
            String tmps = ""; 
            boolean chk = false; 
            boolean firstrun = true; 
            
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				if(line.startsWith("platform_id,date,")){
					continue;
				}
				String[] values = line.split(",");
				
				tmps += line+"\n";
				
				double tmp = Double.parseDouble(values[3]); 
                if(firstrun){ 
                    max = tmp; 
                    maxplus10sec = max + 0.000000010000E9; 
                    firstrun=false; 
                } 
                if(tmp>max){ 
                    max = tmp; // to maintain pidlist.json file;
                } 
                if((maxplus10sec+0.000000005000E9)<tmp){ 
                    maxplus10sec = tmp + 0.000000010000E9; // if there is an interval of more than 5 sec between row; 
                }else if(maxplus10sec<=tmp){ 
                    chk = true; 
                }
                
                if(chk){
                	gc.store(GalileoConnector.createBlock(tmps));
                }
				
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
