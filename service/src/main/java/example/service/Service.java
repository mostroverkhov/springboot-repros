package example.service;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

@Controller
public class Service {
    private static final Logger logger = LoggerFactory.getLogger(Service.class);

    @MessageMapping("channel")
    public Flux<Response> channel(Publisher<Request> request) {
        return Flux.error(new RuntimeException("plain error"));
    }
}
