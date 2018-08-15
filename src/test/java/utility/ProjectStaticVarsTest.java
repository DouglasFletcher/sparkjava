package utility;

import base.config.SJConfig;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;

public class ProjectStaticVarsTest {

    private String projLoc = SJConfig.projectLocDirConfig.getValue();

    private String testFile = "test.csv";

    @Inject
    ProjectStaticVars projectStaticVars;

    @Test
    public void createFileLoc_shouldSucceed(){
        String actual = projectStaticVars.createFileLoc("test.csv");
        String expected = projLoc + testFile;
        // run tests
        assertEquals(expected, actual);
    }

    @Test
    public void getProjloc_shouldSucceed(){
        assertEquals(projLoc, projectStaticVars.getProjloc());
    }

}
