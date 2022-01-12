package com.example.demo;

import com.example.demo.geonames.model.AutoFlushingStreamWrapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.IntStream;
import java.util.stream.Stream;

@RestController
@RequiredArgsConstructor
public class SlowNumbersController {

    /**
     * Long time-to-first-byte, because items are too small to fill the response-buffer.
     */
    @GetMapping("/slowNumbers/plain")
    public Stream<Integer> streamNumbersSlowly() {
        return IntStream.range(0, 10).mapToObj(this::sleep);
    }

    /**
     * Flush response-stream after each number
     */
    @GetMapping("/slowNumbers/wrapped")
    public AutoFlushingStreamWrapper<Integer> streamNumbersSlowlyWithWrapper() {
        return new AutoFlushingStreamWrapper<>(this::streamNumbersSlowly);
    }

    @SneakyThrows
    private int sleep(int number) {
        Thread.sleep(1000);
        return number;
    }
}
