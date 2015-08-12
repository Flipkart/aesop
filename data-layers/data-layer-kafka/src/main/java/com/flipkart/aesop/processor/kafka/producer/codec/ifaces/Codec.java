package com.flipkart.aesop.processor.kafka.producer.codec.ifaces;

import javax.validation.constraints.NotNull;
import java.io.IOException;

public interface Codec<T> {
    /**
     * Encodes the data into byte array
     * @param o the object to encode into
     * @return
     * @throws IOException
     */
    byte[] encode(@NotNull T o) throws IOException;

    /**
     * Decodes the bytes into the Type 'T'
     * @param bytes , the byte array to decode into
     * @return
     * @throws IOException
     */
    T decode(byte[] bytes) throws IOException;
}