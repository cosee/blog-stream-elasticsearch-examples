package com.example.demo.geonames;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

@RequiredArgsConstructor(staticName = "of")
@Log
public class StreamPartitioner<T> {

    final Stream<T> input;
    final int partitionSize;
    final List<T> buffer = new LinkedList<>();

    public void forEach(Consumer<List<T>> consumer) {

        input.forEach(item -> {
            log.info(item.toString());
            buffer.add(item);
            if (buffer.size() >= partitionSize) {
                try {
                    consumer.accept(List.copyOf(buffer));
                } finally {
                    buffer.clear();
                }
            }
        });
        if (!buffer.isEmpty()) {
            consumer.accept(List.copyOf(buffer));
        }
    }

}
