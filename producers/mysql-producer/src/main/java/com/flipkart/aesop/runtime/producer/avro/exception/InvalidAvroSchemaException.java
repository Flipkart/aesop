package com.flipkart.aesop.runtime.producer.avro.exception;

/**
 * Created by akshit.agarwal on 19/04/16.
 */

public class InvalidAvroSchemaException extends RuntimeException
{
    public InvalidAvroSchemaException() {
    }

    public InvalidAvroSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
    public InvalidAvroSchemaException(String message) {
        super(message);
    }
    public InvalidAvroSchemaException(Throwable cause) {
        super(cause);
    }
}
