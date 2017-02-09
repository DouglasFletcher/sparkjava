package creditscoring;

public class ProjectStaticVars {

	public static final String projLoc = "C:/Users/douglas.fletcher/Documents/projects/creditscore_spark/";
	
	public static String createFileLoc(String fileIn){
		// file location
		String outfile = new StringBuilder(projLoc).append(fileIn).toString();
		return outfile ;  
	}

	public static String getProjloc() {
		return projLoc;
	}
		
}
