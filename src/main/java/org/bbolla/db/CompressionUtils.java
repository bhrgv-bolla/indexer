package org.bbolla.db;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Simple string compression.
 * Default charset java uses: utf-8?
 */
@Slf4j
public class CompressionUtils {

    /**
     * Returns a compressed string
     *
     * @param uncompressed
     * @return
     */
    public static String compress(String uncompressed) {

        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            gzipOutputStream.write(uncompressed.getBytes(StandardCharsets.UTF_8));
            gzipOutputStream.finish();
            gzipOutputStream.flush();
            gzipOutputStream.close();
            String compressed = Base64.getUrlEncoder().encodeToString(byteArrayOutputStream.toByteArray());
            logCompressionStats(uncompressed, compressed);
            return compressed;
        } catch (Exception ex) {
            throw new RuntimeException("Error while compressing! cannot compress : ", ex);
        }
    }


    /**
     * Returns an uncompressed string
     *
     * @param compressed
     * @return
     */
    public static String uncompress(String compressed) {
        try {
            log.debug("preview compressed string: {}", compressed.substring(0, 20)); //end index >>>>> greater than 20
            byte[] decoded = Base64.getUrlDecoder().decode(compressed);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(decoded); //uses default charset.
            GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            return IOUtils.toString(gzipInputStream, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new RuntimeException("Error while uncompressing, cannot uncompress", ex);
        }
    }

    private static void logCompressionStats(String input, String compressed) {
        Integer inputSize = input.getBytes().length;
        Integer compressedSize = compressed.getBytes().length;
        double ratio = (double) inputSize / compressedSize;
        log.debug("Compression ratio : {}", ratio);
    }

}