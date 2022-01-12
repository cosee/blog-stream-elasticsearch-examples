package com.example.demo.geonames;

import com.example.demo.geonames.model.PostalCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class ImportIntoElasticSearchService {

    public static final int IMPORT_PAGE_SIZE = 20000;
    private final PostalCodeElasticSearchService postalCodeElasticSearchService;
    private final PostalCodeImportService postalCodeImportService;

    public void executeImport() {
        Stream<PostalCode> input = postalCodeImportService.importFromFile();


        AtomicInteger counter = new AtomicInteger(0);
        StreamPartitioner.of(input, 20000)
                .forEach(postalCodeList -> {
                    postalCodeElasticSearchService.addPostalCodes(postalCodeList);
                    log.info("Imported "+ counter.addAndGet(IMPORT_PAGE_SIZE) + " postal codes");
                });

    }
}
