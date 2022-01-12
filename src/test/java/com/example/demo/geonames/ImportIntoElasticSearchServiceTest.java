package com.example.demo.geonames;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@ExtendWith(SpringExtension.class)
@Disabled("Test usually takes very long")
class ImportIntoElasticSearchServiceTest {

    @Autowired
    private PostalCodeElasticSearchService postalCodeElasticSearchService;

    @Autowired
    private ImportIntoElasticSearchService importIntoElasticSearchService;

    @BeforeEach
    void setUp() {
        postalCodeElasticSearchService.deleteIndex();
    }
    @Test
    void executeImport() {
        importIntoElasticSearchService.executeImport();
        postalCodeElasticSearchService.refresh();
        assertThat(postalCodeElasticSearchService.countAllPostalCodes()).isGreaterThan(100000);
    }
}
