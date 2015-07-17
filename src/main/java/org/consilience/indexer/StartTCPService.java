package org.consilience.indexer;

/**
 * Created by cloudera on 7/13/15.
 */


import org.consilience.helpers.ESKeywords;
import org.consilience.helpers.ESVarNames;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartTCPService implements ElasticSearchClient {
    private static final Logger logger = LoggerFactory.getLogger(StartTCPService.class);
    private Client client;

    public Client getClient() {
        if (client == null)
            client = createClient();
        return client;
    }

    /**
     *
     * @throws Exception
     */
    public void ping() throws Exception {
        if (getClient() == null) {
            System.out.println("[ERROR] Node disconnected.");
            throw new Exception("ElasticSearch client doesn't exist. Your factory is not properly initialized.");
        }
    }

    protected Client createClient() {
        if (client == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Creating client for debug...");
            }
            try {
                Settings settings = ImmutableSettings.settingsBuilder().put(
                        ESKeywords.CLUSTER_NAME.getText(),
                        ESVarNames.CLUSTER_NAME.getText()
                ).build();

                TransportClient transportClient = new TransportClient(settings);
                transportClient = transportClient.addTransportAddress(
                        new InetSocketTransportAddress(
                                ESVarNames.ADDRESS.getText(),
                                Integer.parseInt(ESVarNames.TCP_PORT.getText())
                        )
                );
                if (transportClient.connectedNodes().size() == 0) {
                    logger.error("There are no active nodes available for the transport, it will be automatically added once nodes are live!");
                }
                client = transportClient;
            } catch (Exception ex) {
                logger.error("Error occurred while creating search client!", ex);
            }
        }
        return client;
    }

    public void addNewNode(String name) {
        TransportClient transportClient = (TransportClient) client;
        transportClient.addTransportAddress(
                new InetSocketTransportAddress(
                        name,
                        Integer.parseInt(ESVarNames.TCP_PORT.getText())));
    }

    public void removeNode(String name) {
        TransportClient transportClient = (TransportClient) client;
        transportClient.removeTransportAddress(
                new InetSocketTransportAddress(
                        name,
                        Integer.parseInt(ESVarNames.TCP_PORT.getText())));
    }
}
