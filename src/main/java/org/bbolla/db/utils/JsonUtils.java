package org.bbolla.db.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author bbolla on 9/21/18
 */
public class JsonUtils {


    private static final ObjectMapper objectMapper = new ObjectMapper();

    static  {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }


    public static String serialize(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("Serialization Exception", e);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> toClass) {
        try {
            return objectMapper.readValue(bytes, toClass);
        } catch (IOException e) {
            throw new RuntimeException("Deserialization Exception", e);
        }
    }

    public static <T> T deserialize(String input, Class<T> toClass) {
        try {
            return objectMapper.readValue(input, toClass);
        } catch (IOException e) {
            throw new RuntimeException("Deserialization Exception", e);
        }
    }
}