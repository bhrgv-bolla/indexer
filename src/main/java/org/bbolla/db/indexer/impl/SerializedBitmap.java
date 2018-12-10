package org.bbolla.db.indexer.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;

/**
 * @author bbolla on 11/21/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SerializedBitmap implements Serializable {

    private byte[] serializedBitmap;

    public static SerializedBitmap fromBitMap(Roaring64NavigableMap rr) {
        try {
            //serialize.
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteArray);
            rr.serialize(output);
            output.flush();
            output.close();
            return new SerializedBitmap(byteArray.toByteArray());
        } catch (IOException ex) {
            throw new RuntimeException("Not able to create serialized bitmap from input", ex);
        }
    }

    public Roaring64NavigableMap toBitMap() {
        try {
            Roaring64NavigableMap rr = Roaring64NavigableMap.bitmapOf();
            if (this.serializedBitmap == null) return rr;
            else {
                rr.deserialize(new DataInputStream(new ByteArrayInputStream(this.serializedBitmap)));
                return rr;
            }
        } catch (IOException ex) {
            throw new RuntimeException("Not able to create bitmap", ex);
        }
    }


    public int getSizeInBytes() {
        if(this.serializedBitmap == null) return 0;
        else return this.serializedBitmap.length;
    }
}