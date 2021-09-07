package example.client;

import io.netty.util.ResourceLeakDetector;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Objects;

@SpringBootApplication
public class Main {
    private static final int WINDOW = 7777;

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        SpringApplication.run(Main.class, args);
    }

    @Component
    public static class Runner implements CommandLineRunner {

        @Autowired
        private RSocketRequester rSocketRequester;

        @Override
        public void run(String... args) {
            Request requestMessage = new Request();
            requestMessage.setMessage("data");

            ChannelSubscriber<Response> channelSubscriber = new ChannelSubscriber<>();

            Flux<Request> request =
                    payloadSource(requestMessage).takeUntilOther(channelSubscriber.onCompleted());

            rSocketRequester.route("channel")
                    .data(request)
                    .retrieveFlux(Response.class)
                    .subscribe(channelSubscriber);

            Objects.requireNonNull(rSocketRequester.rsocket()).onClose().block();
        }
    }

    static <T> Flux<T> payloadSource(T t) {
        return Flux.create(
                sink -> {
                    sink.onRequest(
                            value -> {
                                for (long i = 0; i < value; i++) {
                                    sink.next(t);
                                }
                            });
                });
    }

    static class ChannelSubscriber<T> implements CoreSubscriber<T> {
        private final MonoProcessor<Void> onCompleted = MonoProcessor.create();
        private Subscription s;
        private int window = WINDOW;

        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            s.request(WINDOW);
        }

        @Override
        public void onNext(T response) {
            if (--window == WINDOW / 2) {
                window += WINDOW;
                s.request(WINDOW);
            }
        }

        @Override
        public void onError(Throwable t) {
            logger.error("spring-rsocket channel error", t);
            onCompleted.onComplete();
        }

        @Override
        public void onComplete() {
            onCompleted.onComplete();
        }

        public Mono<Void> onCompleted() {
            return onCompleted;
        }
    }
}
