package co.id.jalin.camunda.training.day5.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Log4j2
@Component
public class GenerateJsonResponseWorker {

    private final ObjectMapper mapper = new ObjectMapper();

    @JobWorker(type = "generate_json_response")
    public Map<String, Object> handle(ActivatedJob job) {
        Map<String, Object> vars = new HashMap<>(job.getVariablesAsMap());

        try {
            Map<String, Object> response = new HashMap<>();

            // kumpulkan field penting dari semua worker sebelumnya
            response.put("mode", vars.get("mode"));
            response.put("batchId", vars.get("batchId"));
            response.put("status", vars.get("status"));
            response.put("action", vars.get("action"));
            response.put("info", vars.get("info"));

            // tambahkan data spesifik kalau ada
            if (vars.containsKey("totalAmount")) {
                response.put("totalAmount", vars.get("totalAmount"));
            }
            if (vars.containsKey("duplicates")) {
                response.put("duplicates", vars.get("duplicates"));
                response.put("duplicatesCount", vars.get("duplicatesCount"));
            }
            if (vars.containsKey("savedBatch")) {
                response.put("savedBatch", vars.get("savedBatch"));
            }

            // buat JSON string
            String responseJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);

            vars.put("response", response);
            vars.put("responseJson", responseJson);
            vars.put("status", "OK");

            log.info("🧾 Generated JSON Response:\n{}", responseJson);

        } catch (Exception e) {
            vars.put("status", "ERROR");
            vars.put("error", e.getMessage());
            log.error("❌ Failed to generate response: {}", e.getMessage());
        }

        return vars;
    }
}
