package server.dm.page;


import org.junit.Test;

import java.util.Random;

public class PageOneTest {

    @Test
    public void testPageOne() {
        Random random = new Random();
        byte[] bytes = new byte[8];
        random.nextBytes(bytes);
        System.out.println(bytes);
    }

}