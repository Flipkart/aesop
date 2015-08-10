package com.flipkart.aesop.processor.kafka.producer.codec;

import com.flipkart.aesop.processor.kafka.producer.codec.ifaces.Codec;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.util.Map;


public class MapCodec implements Codec<Map<String,Object>> {

    public byte[] encode(@NotNull Map<String,Object> object) throws IOException {
        if (object == null) {
            return null;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.flush();
        }
        catch (IOException ex) {
            throw new IllegalArgumentException("Failed to serialize object of type: " + object.getClass(), ex);
        }
        return baos.toByteArray();
    }

    @SuppressWarnings(value = "unchecked")
    public Map<String,Object> decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            return null;
        }
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return (Map<String, Object>) ois.readObject();
        }
        catch (IOException ex) {
            throw new IllegalArgumentException("Failed to deserialize object", ex);
        }
        catch (ClassNotFoundException ex) {
            throw new IllegalStateException("Failed to deserialize object type", ex);
        }
    }
}
