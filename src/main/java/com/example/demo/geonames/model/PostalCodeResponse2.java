package com.example.demo.geonames.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class PostalCodeResponse2 {
    private final AutoFlushingStreamWrapper<String> postalCodes;
}
