package dda.pattern.request.client;

import dda.pattern.request.model.Model;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class Consumer {

    @KafkaListener(topics = "${kafka.topic.request-topic}")
    @SendTo
    public Model listen(Model request) throws InterruptedException {

        int sum = request.getFirstNumber() + request.getSecondNumber();
        request.setAdditionalProperty("sum", sum);

        Random random = new Random();
        int i = random.nextInt(1000);
        System.out.println("Sum: " + sum + ". Sleep: " + i);
        Thread.sleep(i);

        return request;
    }
}
