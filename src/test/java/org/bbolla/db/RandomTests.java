package org.bbolla.db;

import com.google.common.base.Stopwatch;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.bbolla.db.indexer.impl.SerializedBitmap;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.xerial.snappy.Snappy;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
        for (int i = 1; i <= PAGE_SIZE; i++) {
            icsvWriter.writeNext(new String[]{String.valueOf(i), fakeRecord()});
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
        for (int i = 0; i < bytes; i++) {
            chars[i] = randChar();
        }
        return new String(chars);
    }


    @Test
    public void testsCompression() throws IOException {
        for (int i = 1; i <= 10000; i = i + 4) {
            testsCompression(i);
        }
    }

    private void testsCompression(int interval) throws IOException {

        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();
        for (long i = 0; i < 25000000L; i++) {
            if (i % interval == 0) rr.addLong(i);
        }
        rr.runOptimize();
        SerializedBitmap sb = SerializedBitmap.fromBitMap(rr);
        Stopwatch stopwatch = Stopwatch.createStarted();
        byte[] compressedSb = Snappy.compress(sb.getBytes());
        stopwatch.stop();
//        log.info("{} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        log.info("Sparseness: {}, Cardinality: {}, Uncompressed Size: {}, Compressed size: {}, Compression Ratio: {}", interval, rr.getLongCardinality(), sb.sizeInBytes(), compressedSb.length, (double) sb.sizeInBytes() / compressedSb.length);
    }


    @Test
    public void testsRawInsertionTime() throws IOException {
        long total = 3000000L;
        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();
        byte[] sb = SerializedBitmap.toByteArray(rr);
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (long i = 0; i < total; i++) {
            rr = SerializedBitmap.toBitMap(sb); //simulating the serializing and deserializing.
            if (i % 5 == 0) rr.addLong(i);
            sb = SerializedBitmap.toByteArray(rr);
        }
        stopwatch.stop();
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        log.info("Raw insertion time for {} records: {} ms, at a rate of: {} records/sec", total, elapsed, total / (elapsed / 1000));
    }


    @Test
    public void testsRawBatchInsertionTime() throws IOException {
        long total = 100_000_000L;
        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();
        byte[] sb = SerializedBitmap.toByteArray(rr);
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (long i = 0; i < total; i++) {
            rr = SerializedBitmap.toBitMap(sb); //simulating the serializing and deserializing.
            for(long j=0 ; i<total && j<100000; j++, i++) if (i % 5 == 0) rr.addLong(i); //batching.
            sb = SerializedBitmap.toByteArray(rr);
        }
        stopwatch.stop();
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        log.info("Raw insertion time for {} records: {} ms, at a rate of: {} records/sec", total, elapsed, (long) (total / ((double) elapsed / 1000)));
    }



    @Test
    public void testsSerDeTime() throws IOException {
        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();
        for(long i=0; i<500000; i++) {
            if(i % 5 ==0) rr.addLong(i);
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        byte[] o = SerializedBitmap.toByteArray(rr);
        SerializedBitmap.toBitMap(o);

        stopwatch.stop();

        log.info("SerDe time : {} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testsRawInsertionTimeRoaring() throws IOException {
        long total = 2000000L;
        RoaringBitmap rr = RoaringBitmap.bitmapOf();
        byte[] sb = SerializedBitmap.toByteArray(rr);
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < total; i++) { //19:44:05.507 [main] INFO org.bbolla.db.RandomTests - Raw insertion time for 3 Billion records: 25005 ms, at a rate of: 120000000 records/sec
            rr = SerializedBitmap.toRoaringBitMap(sb); //simulating the serializing and deserializing.
            if (i % 5 == 0) rr.add(i);
            sb = SerializedBitmap.toByteArray(rr);
        }
        stopwatch.stop();
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        log.info("Raw insertion time for {} records: {} ms, at a rate of: {} records/sec; size: {} bytes", total, elapsed, total / (elapsed / 1000), sb.length);
    }


    @Test
    public void testReadTimes() throws IOException {
        int fileSize = 3;
        //write 20 mb file.
        byte[] contents = new byte[fileSize * 1000 * 1000];
        Arrays.fill(contents, (byte) 'a');
        Files.write(Paths.get("temp"), contents, StandardOpenOption.TRUNCATE_EXISTING);
        //read 20 mb file.

        Stopwatch stopwatch = Stopwatch.createStarted();
        Files.readAllBytes(Paths.get("temp"));
        stopwatch.stop();
        //check it takes less than <10 ms.

        log.info("{} mb Raw file io time : {} ms", fileSize, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private char randChar() {
        int randInt = rand.nextInt(256);
        return (char) randInt;
    }
}
