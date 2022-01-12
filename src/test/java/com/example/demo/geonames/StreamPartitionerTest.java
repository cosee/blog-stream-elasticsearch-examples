package com.example.demo.geonames;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class StreamPartitionerTest {

    @Test
    void partitionsTheStream() {
        List<List<Integer>> forEachCalls = new LinkedList<>();
        StreamPartitioner.of(IntStream.range(0,5).boxed(), 2).forEach(forEachCalls::add);
        assertThat(forEachCalls)
                .containsExactlyInAnyOrder(
                        List.of(0,1),
                        List.of(2,3),
                        List.of(4)
                );

    }

    @Test
    void doesNotCallWithEmptyLists() {
        List<List<Integer>> forEachCalls = new LinkedList<>();
        StreamPartitioner.of(IntStream.range(0,4).boxed(), 2).forEach(forEachCalls::add);
        assertThat(forEachCalls)
                .containsExactlyInAnyOrder(
                        List.of(0,1),
                        List.of(2,3)
                );

    }
}
