package co.id.jalin.camunda.training.day5.worker;

import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class IterateCollectionsWorker {

    @JobWorker(type = "iterate_collections")
    public Map<String, Object> handle(ActivatedJob job) {
        Map<String, Object> vars = new HashMap<>(job.getVariablesAsMap());
        List<Map<String, Object>> listA = (List<Map<String, Object>>) vars.get("parsedPayloadA");
        List<Map<String, Object>> listB = (List<Map<String, Object>>) vars.get("parsedPayloadB");

        Set<String> idsA = listA.stream()
                .map(m -> m.get("trxId").toString())
                .collect(Collectors.toSet());
        Set<String> idsB = listB.stream()
                .map(m -> m.get("trxId").toString())
                .collect(Collectors.toSet());

        List<String> duplicates = idsA.stream()
                .filter(idsB::contains)
                .collect(Collectors.toList());

        vars.put("duplicates", duplicates);
        vars.put("duplicatesCount", duplicates.size());
        vars.put("info", "Found " + duplicates.size() + " duplicate transactions");
        vars.put("action", "compare");
        vars.put("status", "OK");
        return vars;
    }
}
