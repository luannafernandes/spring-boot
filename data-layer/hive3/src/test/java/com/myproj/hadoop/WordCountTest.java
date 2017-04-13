package com.myproj.hadoop;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class WordCountTest {

//    String BROWSER = "Mozilla/5.0";
//    String OS = "(Windows";
//    String ID = BROWSER +
//            " " +
//            OS +
//            " NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1";
//    String NORMAL = "8f50f880444aa39dff8afd286229de8c\t20130311000101091\t1\tb7120570f1868f55505ba61a1b943ab2\t" +
//            ID +
//            "\t222.36.212.*\t2\t2\t2\ttrqRTuM7Gqq7adj4JKTI\t83ecb0982b64877311b600d8d037321\tnull\t2318061531\t250\t250\t2\t3a0cf3767556609a1f4329c9f52e387e\t300\t33\t9f4e2f16b6873a7eb504df6f61b24044";
//
//    String UBNORMAL = "hallo world";

//    @Test
//    public void testREGEXP() throws IOException {
//        Assert.assertEquals(UdfSplitter.getIPinyou(NORMAL).get(), Optional.of(ID).get());
//        Assert.assertFalse(UdfSplitter.getIPinyou(UBNORMAL).isPresent());
//        Assert.assertTrue(UdfSplitter.getEntry(NORMAL).isPresent());
//        Assert.assertEquals(UdfSplitter.getEntry(NORMAL).get().getBrowser(), BROWSER);
//        Assert.assertEquals(UdfSplitter.getEntry(NORMAL).get().getOs(), OS);
//    }

    @Test
    public void testEntry() {
        Assert.assertEquals(UdfSplitter.parseEntry("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)").get().getBrowser(), "Mozilla/4.0");
        Assert.assertEquals(UdfSplitter.parseEntry("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)").get().getOs(), "(compatible;");
    }

}