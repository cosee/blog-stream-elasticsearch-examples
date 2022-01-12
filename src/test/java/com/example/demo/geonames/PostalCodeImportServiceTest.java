package com.example.demo.geonames;

import com.example.demo.geonames.model.PostalCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@ExtendWith(SpringExtension.class)
class PostalCodeImportServiceTest {

    @Autowired
    private PostalCodeImportService postalCodeImportService;

    @Test
    void importFromFile() {
        try (Stream<PostalCode> postalCodeStream = postalCodeImportService.importFromFile()) {
            assertThat(postalCodeStream.limit(20)).contains(
                    PostalCodeTestUtils.buildPostalCode("1","AD", "AD100", "Canillo"),
                    PostalCodeTestUtils.buildPostalCode("9", "AR", "4123", "LAS SALADAS")
            );
        }
    }

}
