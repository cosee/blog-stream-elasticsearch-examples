package com.example.demo.geonames.model;

import lombok.Data;

@Data
public class PostalCode {
    private String id;
    private String countryIso2;
    private String code;
    private String name;
}
