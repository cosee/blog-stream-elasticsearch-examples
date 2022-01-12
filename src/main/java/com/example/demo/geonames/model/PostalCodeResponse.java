package com.example.demo.geonames.model;

import lombok.Data;

import java.util.stream.Stream;

@Data
public class PostalCodeResponse {
    private final Stream<PostalCode> postalCodes;
}
