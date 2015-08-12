package com.flipkart.aesop.processor.kafka.producer.codec;

import com.flipkart.aesop.processor.kafka.producer.codec.ifaces.Codec;
import org.apache.kafka.common.errors.SerializationException;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 *  String encoding defaults to UTF8 and can be customized by setting the property key.serializer.encoding,
 *  value.serializer.encoding or serializer.encoding. The first two take precedence over the last.
 */
public class StringCodec implements Codec<String> {
    private String encoding = "UTF8";
    @Override
    public byte[] encode(@NotNull String data) throws IOException {
        try {
            return data.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
    }

    @Override
    public String decode(byte[] bytes) throws IOException {
        return new String(bytes,encoding);
    }
}
