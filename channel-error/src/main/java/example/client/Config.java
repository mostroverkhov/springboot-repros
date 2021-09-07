package example.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.rsocket.messaging.RSocketStrategiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.cbor.Jackson2CborDecoder;
import org.springframework.http.codec.cbor.Jackson2CborEncoder;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeTypeUtils;

@Configuration
public class Config {

    @Value("${example.service.hello.hostname}")
    private String helloServiceHostname;

    @Value("${example.service.hello.port}")
    private int helloServicePort;

    @Bean
    public RSocketRequester rsocketRequester() {
        return RSocketRequester.builder()
                .rsocketStrategies(RSocketStrategies.builder()
                        .decoder(new Jackson2CborDecoder())
                        .encoder(new Jackson2CborEncoder())
                        .build())
                .connectTcp(helloServiceHostname, helloServicePort)
                .block();
    }
}
