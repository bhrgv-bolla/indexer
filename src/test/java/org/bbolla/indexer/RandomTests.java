package org.bbolla.indexer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;

/**
 * @author bbolla on 12/7/18
 */
@Slf4j
public class RandomTests {

    private static final int PAGE_SIZE = 10000;
    private static final String OUTPUT_PATH = "./demo_pages/test_page.csv";
    private static final Random rand = new Random();

    @Test
    public void setupTestData() throws IOException {
        log.info("fake record: {}", fakeRecord());
        StringWriter records = new StringWriter();
        PrintWriter printWriter = new PrintWriter(records);
        ICSVWriter icsvWriter = new CSVWriterBuilder(printWriter).build();
        for(int i=1; i<=PAGE_SIZE; i++) {
            icsvWriter.writeNext(new String[] {String.valueOf(i), fakeRecord()});
        }
        log.info("Sample records: \n{}", records.toString().substring(0, 100));

        String compressedPage = CompressionUtils.compress(records.toString());
        log.info("Sample records: \n{}", compressedPage.substring(0, 100));

        createDirectory(OUTPUT_PATH);

        Files.write(Paths.get(OUTPUT_PATH, "test.page"), Collections.singleton(compressedPage));
        Files.write(Paths.get(OUTPUT_PATH, "test.page.uncompressed"), Collections.singleton(records.toString()));

        log.info("Length: {}", compressedPage.length());
    }

    private void createDirectory(String path) throws IOException {
        new File(path).mkdirs();
    }

    private String fakeRecord() {
        return randomChars(500);
    }

    private String randomChars(int bytes) {
        char[] chars = new char[bytes];
        for(int i=0; i<bytes; i++) {
            chars[i] = randChar();
        }
        return new String(chars);
    }

    private char randChar() {
        int randInt = rand.nextInt(256);
        return (char) randInt;
    }
}
