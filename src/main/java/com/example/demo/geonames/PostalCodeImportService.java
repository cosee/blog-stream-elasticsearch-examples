package com.example.demo.geonames;

import com.example.demo.geonames.model.PostalCode;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

@Service
public class PostalCodeImportService {

    @SneakyThrows
    public Stream<PostalCode> importFromFile() {
        AtomicInteger idCounter = new AtomicInteger();

        BufferedReader reader = createPostalCodeReader("/allCountries.txt.gz");
        try {
            return reader.lines().map(line -> parsePostalCode(line, idCounter.incrementAndGet())).onClose(() -> {
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignore
                }
            });
        } catch (Exception e) {
            reader.close();
            throw new RuntimeException("Error while reading allCountries file");
        }
    }

    private BufferedReader createPostalCodeReader(String postalCodeFile) throws IOException {
        InputStream postalCodeStream = PostalCodeImportService.class.getResourceAsStream(postalCodeFile);
        assert postalCodeStream != null;
        GZIPInputStream gzipInputStream = new GZIPInputStream(postalCodeStream);
        return new BufferedReader(new InputStreamReader(gzipInputStream));
    }

    private PostalCode parsePostalCode(String line, int idCounter) {
        String[] split = line.split("\t");
        PostalCode postalCode = new PostalCode();
        postalCode.setId(String.valueOf(idCounter));
        postalCode.setCountryIso2(split[0]);
        postalCode.setCode(split[1]);
        postalCode.setName(split[2]);
        return postalCode;
    }
}
