import java.io.*;
/**
 * @author Duck Keun Yang
 * This program run python scripts; CalculateOccassionMaps201405020.py, CalculateOccassionsPart2_20140520.py, CalculatePeakMaps20140629.py in Java
 * 
 * #NEED TO: change scriptsDir (where python scripts are stored)
 * #NEED TO: change parameters in each python script (ex: directory..)
 */
public class BatchRunARCPYScripts {
	static String scriptsDir = "C:/Users/pinkmaggot/Desktop/SummerProject/Sample Python Scripts/";
	public static void main(String[] args) {
		// get which python script will be executed by command line argument.
		if((args.length != 1) || (!(args[0].equals("1")||args[0].equals("2")||args[0].equals("3")))){
			System.out.println("ERROR: Invalid Argument! Input must be 1, 2 or 3.");
			System.out.println("'1':CalculateOccassionMaps201405020.py, '2':CalculateOccassionsPart2_20140520.py, '3':CalculatePeakMaps20140629.py");
			System.exit(0);
		}
		try{
			ProcessBuilder pb;
			if(args[0].equals("1")){
				pb = new ProcessBuilder("python",scriptsDir+"CalculateOccassionMaps201405020.py");
				Process p = pb.start();
			}else if(args[0].equals("2")){
				pb = new ProcessBuilder("python",scriptsDir+"CalculateOccassionsPart2_20140520.py");
				Process p = pb.start();
			}else if(args[0].equals("3")){
				pb = new ProcessBuilder("python",scriptsDir+"CalculatePeakMaps20140629.py");
				Process p = pb.start();
			}else{
				System.out.println("Unexpected Error");
			}
			
		}catch(Exception e){
			System.out.println("ERROR! "+e);
		}
	}
}
