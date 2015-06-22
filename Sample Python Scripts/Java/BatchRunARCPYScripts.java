import java.io.*;
/**
 * @author Duck Keun Yang
 * This program remotely execute python scripts; CalculateOccassionMaps201405020.py, CalculateOccassionsPart2_20140520.py, CalculatePeakMaps20140629.py in Java
 * 
 * #NEED TO: change scriptsDir (where python scripts are stored)
 * #NEED TO: change parameters in each python script (ex: input directory..)
 */
public class BatchRunARCPYScripts {
	static String scriptsDir = "C:/Users/pinkmaggot/Desktop/SummerProject/CSUMS_SummerProject/Sample Python Scripts/";
	public static void main(String[] args) {
		// get which python script will be executed by command line argument.
		if((args.length != 1) || (!(args[0].equals("1")||args[0].equals("2")||args[0].equals("3")))){
			System.out.println("ERROR: Invalid Argument! Input must be 1, 2 or 3.");
			System.out.println("'1':CalculateOccassionMaps201405020.py, '2':CalculateOccassionsPart2_20140520.py, '3':CalculatePeakMaps20140629.py");
			System.exit(0);
		}
		try{
			ProcessBuilder pb;
			Process p=null;
			if(args[0].equals("1")){
				pb = new ProcessBuilder("python",scriptsDir+"CalculateOccassionMaps201405020.py");
				p = pb.start();
				System.out.println("1:CalculateOccassionMaps201405020.py has executed.\n");
			}else if(args[0].equals("2")){
				pb = new ProcessBuilder("python",scriptsDir+"CalculateOccassionsPart2_20140520.py");
				p = pb.start();
				System.out.println("2:CalculateOccassionsPart2_20140520.py has executed.\n");
			}else if(args[0].equals("3")){
				pb = new ProcessBuilder("python",scriptsDir+"CalculatePeakMaps20140629.py");
				p = pb.start();
				System.out.println("3:CalculatePeakMaps20140629.py has executed.\n");
			}else{
				System.out.println("Unexpected Error");
			}
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
	        String s = "";
            System.out.println("Here is the standard output of the command:");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            System.out.println("");
            // read any errors from the attempted command
            System.out.println("Here is the standard error of the command (if any):");
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }
		}catch(Exception e){
			System.out.println("ERROR! "+e);
		}
	}
}
