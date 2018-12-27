package org.bbolla.db.indexer.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;

/**
 * @author bbolla on 11/21/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SerializedBitmap implements Serializable {

    private byte[] bytes;

    public static SerializedBitmap fromBitMap(Roaring64NavigableMap rr) {
        try {//serialize
            return new SerializedBitmap(toByteArray(rr));
        } catch (IOException ex) {
            throw new RuntimeException("Not able to create serialized bitmap from input", ex);
        }
    }

    public static byte[] toByteArray(Roaring64NavigableMap rr) throws IOException {
        rr.runOptimize();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteStream);
        rr.serialize(output);
        output.flush();
        output.close();
        byte[] byteArray = byteStream.toByteArray();
        return byteArray;
    }

    public static SerializedBitmap empty() {
        return new SerializedBitmap();
    }

    public static byte[] toByteArray(RoaringBitmap rr) throws IOException {
        rr.runOptimize();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteStream);
        rr.serialize(output);
        output.flush();
        output.close();
        byte[] byteArray = byteStream.toByteArray();
        return byteArray;
    }

    public static RoaringBitmap toRoaringBitMap(byte[] bytes) throws IOException {
        RoaringBitmap rr = RoaringBitmap.bitmapOf();
        rr.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
        return rr;
    }

    public Roaring64NavigableMap toBitMap() {
        try {
            Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();
            if (this.bytes == null) return rr;
            else {
                return toBitMap(this.bytes);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Not able to create bitmap", ex);
        }
    }

    public static Roaring64NavigableMap toBitMap(byte[] bytes) throws IOException {
        Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();
        rr.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
        return rr;
    }


    @JsonIgnore
    public int sizeInBytes() {
        if(this.bytes == null) return 0;
        else return this.bytes.length;
    }

}
