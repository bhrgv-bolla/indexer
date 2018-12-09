package org.bbolla.indexer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * @author bbolla on 12/7/18
 */
@Slf4j
public class RandomTests {

    private static final int PAGE_SIZE = 10000;
    private static final String OUTPUT_PATH = "./demo_pages/test_page.csv";

    @Test
    public void setupTestData() throws IOException {
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

        log.info("Length: {}", compressedPage.length());
    }

    private void createDirectory(String path) throws IOException {
        new File(path).mkdirs();
    }

    private String fakeRecord() {
        return "some record data here";
    }
}
