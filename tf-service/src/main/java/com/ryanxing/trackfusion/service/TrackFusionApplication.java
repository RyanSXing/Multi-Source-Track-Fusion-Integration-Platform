package com.ryanxing.trackfusion.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TrackFusionApplication {
    private TrackFusionApplication() {}

    public static void main(String[] args) {
        SpringApplication.run(TrackFusionApplication.class, args);
    }
}
