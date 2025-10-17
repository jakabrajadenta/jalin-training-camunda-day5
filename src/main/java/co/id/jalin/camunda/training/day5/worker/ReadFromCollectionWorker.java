package co.id.jalin.camunda.training.day5.worker;

import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ReadFromCollectionWorker {

    @JobWorker(type = "read_from_collection")
    public Map<String, Object> handle(ActivatedJob job) {
        Map<String, Object> vars = new HashMap<>(job.getVariablesAsMap());
        List<Map<String, Object>> data = (List<Map<String, Object>>) vars.get("parsedPayloadA");

        double total = 0;
        if (data != null) {
            for (Map<String, Object> item : data) {
                Object amt = item.get("amount");
                if (amt != null) total += Double.parseDouble(amt.toString());
            }
        }
        vars.put("totalAmount", total);
        vars.put("info", "Read batch " + vars.get("batchId") + " with total " + total);
        vars.put("action", "read");
        vars.put("status", "OK");
        return vars;
    }
}
