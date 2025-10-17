package co.id.jalin.camunda.training.day5.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ParseJsonWorker {

    private final ObjectMapper mapper = new ObjectMapper();

    @JobWorker(type = "parse_json")
    public Map<String, Object> handle(ActivatedJob job) {
        Map<String, Object> vars = new HashMap<>(job.getVariablesAsMap());
        try {
            // parse payloadA dan payloadB jika ada
            if (vars.containsKey("payloadA")) {
                String jsonA = String.valueOf(vars.get("payloadA"));
                List<Map<String, Object>> listA = mapper.readValue(jsonA, new TypeReference<>() {});
                vars.put("parsedPayloadA", listA);
            }
            if (vars.containsKey("payloadB")) {
                String jsonB = String.valueOf(vars.get("payloadB"));
                List<Map<String, Object>> listB = mapper.readValue(jsonB, new TypeReference<>() {});
                vars.put("parsedPayloadB", listB);
            }
            vars.put("parsed", true);
            vars.put("status", "OK");
        } catch (Exception e) {
            vars.put("parsed", false);
            vars.put("status", "ERROR");
            vars.put("error", e.getMessage());
        }
        return vars;
    }
}
