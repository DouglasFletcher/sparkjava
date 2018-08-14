package utility;

public class ProjectStaticVars {

    public static final String projLoc = "C:/Users/dofletcher/Documents/deloitte/projects/creditscoring/";

    public static String createFileLoc(String fileIn){
        // file location
        String outfile = new StringBuilder(projLoc).append(fileIn).toString();
        return outfile ;
    }

    public static String getProjloc() {
        return projLoc;
    }

}
