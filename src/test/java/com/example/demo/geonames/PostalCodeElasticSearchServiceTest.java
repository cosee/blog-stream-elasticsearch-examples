package com.example.demo.geonames;

import com.example.demo.geonames.model.PostalCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class PostalCodeElasticSearchServiceTest {

    @Autowired
    private PostalCodeElasticSearchService postalCodeElasticSearchService;
    public static final List<PostalCode> TWO_HUNDRED_POSTAL_CODES = IntStream.range(0, 200)
            .boxed()
            .map(i -> PostalCodeTestUtils.buildPostalCode(String.valueOf(i), "DE", String.valueOf(i), "city " + i))
            .collect(Collectors.toList());

    @BeforeEach
    void setUp() {
        postalCodeElasticSearchService.deleteIndex();
    }

    @Test
    void addAndFetch() {
        postalCodeElasticSearchService.addPostalCodes(List.of(
                PostalCodeTestUtils.buildPostalCode("1", "DE", "64289", "Darmstadt"),
                PostalCodeTestUtils.buildPostalCode("2", "DE", "11111", "Nowhere"),
                PostalCodeTestUtils.buildPostalCode("3", "DE", "63165", "Mühlheim")
        ));

        postalCodeElasticSearchService.refresh();

        assertThat(postalCodeElasticSearchService.fetchAllPostalCodes("DE")).containsExactlyInAnyOrder(
                PostalCodeTestUtils.buildPostalCode("1", "DE", "64289", "Darmstadt"),
                PostalCodeTestUtils.buildPostalCode("2", "DE", "11111", "Nowhere"),
                PostalCodeTestUtils.buildPostalCode("3", "DE", "63165", "Mühlheim")
        );

        assertThat(postalCodeElasticSearchService.countAllPostalCodes()).isEqualTo(3);
    }

    @Test
    void addAndFetchByScrollScan() {
        postalCodeElasticSearchService.addPostalCodes(TWO_HUNDRED_POSTAL_CODES);
        postalCodeElasticSearchService.refresh();
        assertThat(postalCodeElasticSearchService.fetchAllPostalCodesByScrollScan("DE")).containsExactlyInAnyOrderElementsOf(TWO_HUNDRED_POSTAL_CODES);
    }

    @Test
    void addAndStreamAllPostalCodesByScrollScan() {

        postalCodeElasticSearchService.addPostalCodes(TWO_HUNDRED_POSTAL_CODES);
        postalCodeElasticSearchService.refresh();
        assertThat(postalCodeElasticSearchService.streamAllPostalCodesByScrollScan("DE"))
                .containsExactlyInAnyOrderElementsOf(TWO_HUNDRED_POSTAL_CODES);
    }

}
