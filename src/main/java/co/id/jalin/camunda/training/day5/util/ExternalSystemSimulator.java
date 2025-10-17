package co.id.jalin.camunda.training.day5.util;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@AllArgsConstructor
public class ExternalSystemSimulator {

    private KafkaTemplate<String,String> kafkaTemplate;

    @KafkaListener(topics = "payment.request", groupId = "simulator-group")
    public void listen(String orderId) throws InterruptedException {
        log.info("Processing order : {}",orderId);
        Thread.sleep(3000);
        if (orderId.contains("ajojing")) {
            kafkaTemplate.send("payment.result","payment failed for " + orderId);
        } else {
            kafkaTemplate.send("payment.result","payment success for " + orderId);
        }
        log.info("Result sent to kafka");
    }
}
