package org.consilience.service.setup;

/**
 * Created by cloudera on 7/13/15.
 */

import java.util.List;
import java.util.Map;

public interface SetupService {
    void initIndex();
    boolean isIndexExists() throws Exception;
    void deleteIndex();
    void updateIndexSettings(Map<String, Object> settings);
    void addAnalyzer(String analyzerName, String analyzerType, String tokenizer, List<String> filter);
    void refreshServer();
}
