package com.ryanxing.trackfusion.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

class TrackFusionApplicationTest {

    @Test
    void startsWithoutExternalInfrastructure() throws Exception {
        Class<?> applicationClass =
                Class.forName("com.ryanxing.trackfusion.service.TrackFusionApplication");

        assertThat(applicationClass).hasAnnotation(SpringBootApplication.class);

        SpringApplication application = new SpringApplication(applicationClass);
        application.setDefaultProperties(
                java.util.Map.of("spring.main.web-application-type", "none"));
        try (ConfigurableApplicationContext context =
                application.run(
                        "--track-fusion.pipeline.enabled=false",
                        "--spring.sql.init.mode=never")) {
            assertThat(context.isActive()).isTrue();
        }
    }
}
