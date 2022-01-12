package com.example.demo.geonames.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;


public class AutoFlushingStreamWrapper<T> implements JsonSerializable {

    private final Supplier<Stream<T>> streamSupplier;

    public AutoFlushingStreamWrapper(Supplier<Stream<T>> streamSupplier) {
        this.streamSupplier = streamSupplier;
    }

    @Override
    public void serialize(JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartArray();
        try (Stream<T> stream = streamSupplier.get()) {
            writeAndFlushRegularly(jsonGenerator, serializerProvider, stream);
        }
        jsonGenerator.writeEndArray();
    }

    private void writeAndFlushRegularly(JsonGenerator jsonGenerator, SerializerProvider serializerProvider, Stream<T> stream) throws IOException {
        Iterator<T> iterator = stream.iterator();
        while (iterator.hasNext()) {
            serializerProvider.defaultSerializeValue(iterator.next(), jsonGenerator);
            jsonGenerator.flush();
        }
    }

    @Override
    public void serializeWithType(JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
        serialize(jsonGenerator, serializerProvider);
    }
}
