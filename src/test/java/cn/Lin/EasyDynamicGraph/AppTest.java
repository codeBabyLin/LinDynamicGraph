package cn.Lin.EasyDynamicGraph;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.nio.file.Paths;
import java.util.HashMap;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void Pathtest(){
        String path = "jkjkjk";
        String path2 = Paths.get(path,"pppppppp").toString();
        HashMap<String,Integer> testMap = new HashMap<>();
        testMap.put("1",2);

        testMap.get("2");

        System.out.println(testMap.get("2"));
        System.out.println(path2);
    }


}
