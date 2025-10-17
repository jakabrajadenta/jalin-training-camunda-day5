package co.id.jalin.camunda.training.day5.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
@Component
public class CreateOrEditCollectionWorker {

    private final ObjectMapper mapper = new ObjectMapper();

    @JobWorker(type = "create_or_edit_collection")
    public Map<String, Object> handle(ActivatedJob job) {
        Map<String, Object> vars = new HashMap<>(job.getVariablesAsMap());
        String batchId = String.valueOf(vars.getOrDefault("batchId", "UNKNOWN"));

        try {
            // ambil payloadA (string JSON array)
            String json = String.valueOf(vars.get("payloadA"));
            List<Map<String, Object>> list = mapper.readValue(json, new TypeReference<>() {});

            // tambahkan info waktu & batch id pada setiap transaksi
            for (Map<String, Object> item : list) {
                item.put("batchId", batchId);
                item.put("processedAt", LocalDateTime.now().toString());
            }

            // simpan kembali ke variabel
            vars.put("savedBatch", list);
            vars.put("action", "upsert");
            vars.put("status", "OK");
            vars.put("info", "Created/Updated batch " + batchId + " with " + list.size() + " records");

            // simpan juga ke file JSON (opsional)
            File outFile = new File("/tmp/" + batchId + "_batch.json");
            mapper.writerWithDefaultPrettyPrinter().writeValue(outFile, list);

            log.info("✅ Batch {} saved to {}", batchId, outFile.getAbsolutePath());

        } catch (Exception e) {
            vars.put("status", "ERROR");
            vars.put("error", e.getMessage());
            log.error("❌ Failed to create/update batch {}", batchId, e);
        }

        return vars;
    }
}
