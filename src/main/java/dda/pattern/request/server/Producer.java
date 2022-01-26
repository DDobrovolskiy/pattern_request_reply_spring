package dda.pattern.request.server;

import dda.pattern.request.model.Model;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.util.Random;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping(value = "/send", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
public class Producer {

    @Autowired
    ReplyingKafkaTemplate<String, Model,Model> replyingKafkaTemplate;

    @Value("${kafka.topic.request-topic}")
    String requestTopic;

    @Value("${kafka.topic.requestreply-topic}")
    String requestReplyTopic;

    @PostMapping("/sum")
    public Model sum(@RequestBody Model request) throws InterruptedException, ExecutionException {
        // create producer record
        ProducerRecord<String, Model> record = new ProducerRecord<String, Model>(requestTopic, request);
        // set reply topic in header
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        // post in kafka topic
        RequestReplyFuture<String, Model, Model> sendAndReceive = replyingKafkaTemplate.sendAndReceive(record);

        // confirm if producer produced successfully
        SendResult<String, Model> sendResult = sendAndReceive.getSendFuture().get();

        //print all headers
        sendResult.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));

        // get consumer record
        ConsumerRecord<String, Model> consumerRecord = sendAndReceive.get();
        // return consumer value
        return consumerRecord.value();
    }

    @GetMapping
    public Model get() throws InterruptedException, ExecutionException {
        Model model = new Model();
        Random random = new Random();

        int i1 = random.nextInt(10);
        int i2 = random.nextInt(10);

        model.setFirstNumber(i1);
        model.setSecondNumber(i2);

        // create producer record
        ProducerRecord<String, Model> record = new ProducerRecord<String, Model>(requestTopic, model);
        // set reply topic in header
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        // post in kafka topic
        RequestReplyFuture<String, Model, Model> sendAndReceive = replyingKafkaTemplate.sendAndReceive(record);

        // confirm if producer produced successfully
        SendResult<String, Model> sendResult = sendAndReceive.getSendFuture().get();

        // get consumer record
        ConsumerRecord<String, Model> consumerRecord = sendAndReceive.get();

        System.out.println(i1 + "" + i2 + " = " + consumerRecord.value().getFirstNumber() + consumerRecord.value().getSecondNumber());

        // return consumer value
        return consumerRecord.value();
    }
}
