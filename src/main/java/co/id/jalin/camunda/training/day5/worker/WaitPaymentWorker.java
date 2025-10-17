package co.id.jalin.camunda.training.day5.worker;

import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Log4j2
@Component
public class WaitPaymentWorker {

    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    @KafkaListener(topics = "payment.result", groupId = "camunda-group")
    public void listen(String message){
        log.info("Received message from kafka : {}",message);
        queue.offer(message.toLowerCase().contains("success") ? "success" : "failed");
    }

    @JobWorker(type = "wait-result")
    public Map<String,Object> handleWait(ActivatedJob job) throws InterruptedException {
        log.info("Waiting for kafka result");
        String result = queue.take();
        Map<String,Object> vars = job.getVariablesAsMap();
        vars.put("result",result);
        return vars;
    }
}
