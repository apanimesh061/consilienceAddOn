package org.consilience.indexer;

/**
 * Created by cloudera on 7/13/15.
 */

import org.elasticsearch.client.Client;

public interface ElasticSearchClient {
    Client getClient();
    void addNewNode(String name);
    void removeNode(String name);
}
