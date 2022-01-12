package com.example.demo.geonames;

import com.example.demo.geonames.model.PostalCode;

public class PostalCodeTestUtils {
    public static PostalCode buildPostalCode(String id, String countryIso2, String code, String name) {
        PostalCode postalcode = new PostalCode();
        postalcode.setId(id);
        postalcode.setName(name);
        postalcode.setCode(code);
        postalcode.setCountryIso2(countryIso2);
        return postalcode;
    }
}
