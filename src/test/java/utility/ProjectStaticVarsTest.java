package utility;

import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

public class ProjectStaticVarsTest {

    private String projLoc = "C:/Users/dofletcher/Documents/deloitte/projects/creditscoring/";

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
