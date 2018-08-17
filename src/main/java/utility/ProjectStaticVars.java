package utility;

import base.config.SJConfig;

/**
 * <h3>Project Static variables</h3>
 * <p>class to store project location variables.</p>
 */
public class ProjectStaticVars {

    public static final String projLoc = SJConfig.projectLocDirConfig.getValue();

    /**
     * <h3>File location</h3>
     * <p>Instance for storing file location parameters</p>
     * @param fileIn String: file location
     * @return outfile: file location
     */
    public static String createFileLoc(String fileIn){
        // file location
        String outfile = new StringBuilder(projLoc).append(fileIn).toString();
        return outfile ;
    }

    /**
     * <h3>get project location</h3>
     * <p>get project location</p>
     * @return projLoc String: get project location
     */
    public static String getProjloc() {
        return projLoc;
    }

}
