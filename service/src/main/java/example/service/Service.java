package example.service;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
public class Service {
    private static final Logger logger = LoggerFactory.getLogger(Service.class);

    @MessageMapping("channel")
    public Flux<Response> channel(Publisher<Request> request) {
        return Flux.error(new RuntimeException("plain error"));
    }

    @MessageMapping("slow-channel")
    public Mono<Response> slowChannel(Publisher<Request> request) {
        Response r = new Response();
        r.setMessage("slow-channel");
        MonoProcessor<Response> response = MonoProcessor.create();
        Flux.from(request).subscribe(new Subscriber<Request>() {
            private final AtomicInteger counter = new AtomicInteger();
            private Disposable disposable;

            @Override
            public void onSubscribe(Subscription s) {
                int requestN = 5;
                s.request(requestN);
                disposable = Flux.interval(Duration.ofSeconds(1))
                        .subscribe(v -> {
                            int payloadCount = counter.getAndSet(0);
                            logger.info("payload requested: {}, received: {}", requestN, payloadCount);
                            s.request(requestN);
                        });
            }

            @Override
            public void onNext(Request request) {
                counter.incrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
                disposable.dispose();
                response.onError(t);
            }

            @Override
            public void onComplete() {
                disposable.dispose();
                response.onComplete();
            }
        });
        return response;
    }

    @MessageMapping("stream")
    public Flux<Response> streamOverflow(Request request) {
        return Flux.create(sink -> {
            sink.onRequest(requestN -> {
                for (long i = 0; i < requestN; i++) {
                    Response r = new Response();
                    r.setMessage(request.getMessage());
                    sink.next(r);
                }
            });
        });
    }

    @MessageMapping("response")
    public Mono<Response> responseOverflow(Request request) {
        Response r = new Response();
        r.setMessage(request.getMessage());
        return Mono.just(r);
    }
}
