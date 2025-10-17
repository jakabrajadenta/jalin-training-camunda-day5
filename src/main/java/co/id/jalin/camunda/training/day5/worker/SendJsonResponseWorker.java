package co.id.jalin.camunda.training.day5.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.io.File;

@Log4j2
@Component
public class SendJsonResponseWorker {

    private final ObjectMapper mapper = new ObjectMapper();

    @JobWorker(type = "send_json_response")
    public void handle(ActivatedJob job) {
        var vars = job.getVariablesAsMap();

        try {
            String batchId = String.valueOf(vars.getOrDefault("batchId", "UNKNOWN"));
            Object response = vars.get("response");

            // Simpan ke file JSON
            File outFile = new File("/tmp/response_" + batchId + ".json");
            mapper.writerWithDefaultPrettyPrinter().writeValue(outFile, response);

            log.info("📤 Response JSON for batch {} saved to {}", batchId, outFile.getAbsolutePath());
            log.info("✅ Response Content:\n{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(response));

        } catch (Exception e) {
            log.error("❌ Failed to send/save response: {}", e.getMessage());
        }
    }
}
