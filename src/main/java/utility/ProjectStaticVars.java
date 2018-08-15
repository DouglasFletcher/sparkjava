package utility;

import base.config.SJConfig;

public class ProjectStaticVars {

    public static final String projLoc = SJConfig.projectLocConfig.getValue();

    public static String createFileLoc(String fileIn){
        // file location
        String outfile = new StringBuilder(projLoc).append(fileIn).toString();
        return outfile ;
    }

    public static String getProjloc() {
        return projLoc;
    }

}
