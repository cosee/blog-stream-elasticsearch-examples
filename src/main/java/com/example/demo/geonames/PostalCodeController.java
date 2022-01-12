package com.example.demo.geonames;

import com.example.demo.geonames.model.PostalCode;
import com.example.demo.geonames.model.PostalCodeResponse;
import com.example.demo.geonames.model.AutoFlushingStreamWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Stream;

@RestController
@RequiredArgsConstructor
public class PostalCodeController {

    private final ImportIntoElasticSearchService importIntoElasticSearchService;
    private final PostalCodeElasticSearchService postalCodeElasticSearchService;

    // import from src/main/resources/allCountries.txt
    @PostMapping("/postal-codes")
    public void importAllFromAllCountriesTxt() {
        importIntoElasticSearchService.executeImport();
    }

    @DeleteMapping("/postal-codes")
    public void deleteAll() {
        postalCodeElasticSearchService.deleteIndex();
    }

    /**
     * Collects all results into a list, which is returned afterwards: Long time-to-first-byte.
     */
    @GetMapping("/postal-codes/{country}/buffered")
    public List<PostalCode> fetchAllBuffered(@PathVariable("country") String countryIso2) {
        return postalCodeElasticSearchService.fetchAllPostalCodesByScrollScan(countryIso2);
    }

    /**
     * Uses a stream as response-type, which causes the result to be streamed (i.e.
     * sent back in the response while the scroll-scan is running
     */
    @GetMapping("/postal-codes/{country}/streamed")
    public Stream<PostalCode> fetchAllStreamed(@PathVariable("country") String countryIso2) {
        return postalCodeElasticSearchService.streamAllPostalCodesByScrollScan(countryIso2);
    }

    /**
     * This endpoint uses a stream-wrapper that flushes the stream after each item
     */
    @GetMapping("/postal-codes/{country}/stream-wrapped")
    public AutoFlushingStreamWrapper<PostalCode> fetchAllStreamWrapped(@PathVariable("country") String countryIso2) {
        return new AutoFlushingStreamWrapper<>(() -> postalCodeElasticSearchService.streamAllPostalCodesByScrollScan(countryIso2));
    }

    /**
     * The postalCodes field in {@link PostalCodeResponse} is of type "Stream<PostalCode>", which will cause
     * the field to be streamed
     */
    @GetMapping("/postal-codes/{country}")
    public PostalCodeResponse fetchAllStreamedAsProperty(@PathVariable("country") String countryIso2) {
        return new PostalCodeResponse(postalCodeElasticSearchService.streamAllPostalCodesByScrollScan(countryIso2));
    }
}
