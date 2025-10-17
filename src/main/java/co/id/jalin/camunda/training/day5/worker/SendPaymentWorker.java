package co.id.jalin.camunda.training.day5.worker;

import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Log4j2
@Component
@AllArgsConstructor
public class SendPaymentWorker {

    private KafkaTemplate<String,String> kafkaTemplate;

    @JobWorker(type = "send-request")
    public void handleSend(ActivatedJob job){
        Map<String,Object> vars = job.getVariablesAsMap();
        var orderId = (String) vars.get("orderId");
        kafkaTemplate.send("payment.request",orderId);
        log.info("Sent payment request : {}", orderId);
    }
}
