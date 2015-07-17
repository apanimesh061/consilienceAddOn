package org.consilience.service;

/**
 * Created by cloudera on 7/13/15.
 */

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.*;

import org.consilience.helpers.ESKeywords;
import org.consilience.helpers.ESVarNames;
import org.consilience.service.setup.SetupService;
import org.consilience.indexer.StartTCPService;
import org.consilience.conf.IndexConfig;
import org.consilience.conf.IndexConfig.SupportedLocale;

import org.consilience.service.setup.templates.Template;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImplementIndex implements SetupService {
    private static final Logger logger = LoggerFactory.getLogger(ImplementIndex.class);

    private StartTCPService client = new StartTCPService();
    public IndexConfig Config;

    /**
     *
     * @param MongoID
     */
    private void setIndexConf(String MongoID) {
        System.out.println("Loading the configuration for current index...");
        String indexName = generateIndexName(MongoID);
        SupportedLocale locale = SupportedLocale.ENGLISH;
        this.Config = new IndexConfig(indexName, locale);
        System.out.printf("Index name set to [%s] with locale as [%s] using text [%s]\n", indexName, locale.getLang(), locale.getText());
    }

    /**
     *
     * @return name of the current index
     */
    private String generateIndexName(String MongoID) {
        return ESVarNames.INDEX_PREFIX.getText() + MongoID;
    }

    /**
     *
     * @return true iff the specified index exists
     * @throws Exception
     */
    public boolean isIndexExists() throws Exception {
        client.ping();
        boolean exists = client.getClient().admin().indices().prepareExists(Config.getIndexName()).execute().actionGet().isExists();
        if (exists) {
            System.out.printf("The index [%s] already exists\n", Config.getIndexName());
        }
        return exists;
    }

    /**
     *
     */
    public void deleteIndex() {
        DeleteIndexResponse deleteIndexResponse = client.getClient().admin().indices().prepareDelete(Config.getIndexName()).execute().actionGet();
        if(deleteIndexResponse.isAcknowledged())
            System.out.println("Index [" + Config.getIndexName() + "] deleted");
    }

    /**
     *  Refreshes the state of the cluster so that
     *  the recent changes are reflected
     */
    public void refreshServer() {
        client.getClient().admin().indices().refresh(Requests.refreshRequest(Config.getIndexName())).actionGet();
        System.out.println(Config.getIndexName() + " refreshed....");
    }

    /**
     *
     * @throws Exception
     */
    public void initIndex() {
        Template newTemplate = new Template(client, Config, ESVarNames.TEMPLATE_NAME.getText(), false);
        try {
            newTemplate.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            CreateIndexResponse createIndexResponse = client.getClient().admin().indices().prepareCreate(Config.getIndexName()).execute().actionGet();
            if (!createIndexResponse.isAcknowledged()) {
                System.out.println("[ERROR] Could not create " + Config.getIndexName() + " index");
            }
        } catch (Exception e) {
            System.out.printf("[ERROR] Index %s could not be created!\n", Config.getIndexName());
            throw new RuntimeException("Error occurred while generating settings for index", e);
        }
    }

    /**
     *
     * @param analyzerName  name of the new analyzer
     * @param analyzerType  type of the analyzer
     * @param tokenizer     type of the tokenizer
     * @param filter        type of the term filter
     * @return              analyzer JSON as a string
     */
    private String registerNewAnalyzer(String analyzerName, String analyzerType, String tokenizer, List<String> filter) {
        XContentBuilder xContentBuilder;
        try {
            xContentBuilder = jsonBuilder()
                .startObject()
                    .startObject(ESKeywords.ANALYSIS.getText())
                        .startObject(ESKeywords.ANALYZER.getText())
                            .startObject(analyzerName)
                                .field(ESKeywords.TYPE.getText(), analyzerType)
                                .field(ESKeywords.TOKENIZER.getText(), tokenizer)
                                .field(ESKeywords.FILTER.getText(), filter)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            ;
            //Settings settings = settingsBuilder().loadFromSource(xContentBuilder.toString()).build();
            return xContentBuilder.prettyPrint().string();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     *
     * @param analyzerName
     */
    public void updateMapping(String analyzerName, double max_freq, double min_freq) {
        try {
            XContentBuilder xContentBuilder = jsonBuilder()
                    .startObject()
                            .startObject(ESVarNames.INDEX_DOC_TYPE_NAME.getText())
                                .field(ESKeywords.DYNAMIC.getText(), ESKeywords.STRICT.getText())
                                .startObject(ESKeywords.PROPERTIES.getText())
                                    .startObject(ESVarNames.DOCSET_ID.getText())
                                        .field(ESKeywords.INCLUDE_IN_ALL.getText(), false)
                                        .field(ESKeywords.TYPE.getText(), ESKeywords.STRING.getText())
                                        .field(ESKeywords.INDEX.getText(), ESKeywords.NOT_ANALYZED.getText())
                                    .endObject()
                                    .startObject(ESVarNames.DOC_ID.getText())
                                        .field(ESKeywords.INCLUDE_IN_ALL.getText(), false)
                                        .field(ESKeywords.TYPE.getText(), ESKeywords.STRING.getText())
                                        .field(ESKeywords.INDEX.getText(), ESKeywords.NOT_ANALYZED.getText())
                                    .endObject()
                                    .startObject(ESVarNames.PLAIN_TEXT.getText())
                                        .field(ESKeywords.TYPE.getText(), ESKeywords.STRING.getText())
                                        .startObject(ESKeywords.FIELDDATA.getText())
                                            .startObject(ESKeywords.FILTER.getText())
                                                .startObject(ESKeywords.FREQUENCY.getText())
                                                    .field(ESKeywords.MAX_FREQUENCY.getText(), max_freq)
                                                    .field(ESKeywords.MIN_FREQUENCY.getText(), min_freq)
                                                .endObject()
                                            .endObject()
                                        .endObject()
                                        .field(ESKeywords.STORE.getText(), true)
                                        .field(ESKeywords.TERM_VECTOR.getText(), ESKeywords.WITH_POSITION_OFFSETS.getText())
                                        .field(ESKeywords.ANALYZED.getText(), true)
                                        .field(ESKeywords.INDEX_ANALYZER.getText(), analyzerName)
                                        .field(ESKeywords.SEARCH_ANALYZER.getText(), analyzerName)
                                    .endObject()
                                    .startObject(ESVarNames.PDF_TEXT.getText())
                                        .field(ESKeywords.TYPE.getText(), ESKeywords.ATTACHMENT.getText())
                                        .startObject(ESKeywords.FIELDS.getText())
                                            .startObject(ESVarNames.PDF_TEXT.getText())
                                                .field(ESKeywords.TYPE.getText(), ESKeywords.STRING.getText())
                                                .startObject(ESKeywords.FIELDDATA.getText())
                                                    .startObject(ESKeywords.FILTER.getText())
                                                        .startObject(ESKeywords.FREQUENCY.getText())
                                                            .field(ESKeywords.MAX_FREQUENCY.getText(), max_freq)
                                                            .field(ESKeywords.MIN_FREQUENCY.getText(), min_freq)
                                                        .endObject()
                                                    .endObject()
                                                .endObject()
                                                .field(ESKeywords.STORE.getText(), true)
                                                .field(ESKeywords.TERM_VECTOR.getText(), ESKeywords.WITH_POSITION_OFFSETS.getText())
                                                .field(ESKeywords.ANALYZED.getText(), true)
                                                .field(ESKeywords.INDEX_ANALYZER.getText(), analyzerName)
                                                .field(ESKeywords.SEARCH_ANALYZER.getText(), analyzerName)
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                    .endObject()
                ;

            //System.out.println(xContentBuilder.prettyPrint().string());

            PutMappingResponse putMappingResponse = client.getClient().admin().indices()
                                                        .preparePutMapping(Config.getIndexName())
                                                        .setType(ESVarNames.INDEX_DOC_TYPE_NAME.getText())
                                                        .setSource(xContentBuilder)
                                                        .execute().actionGet();
            if(putMappingResponse.isAcknowledged()) {
                System.out.printf("[SUCCESS] Added mapping for document set \"%s\" to index \"%s\"...\n",
                        ESVarNames.INDEX_DOC_TYPE_NAME.getText(),
                        Config.getIndexName()
                );
            }
        } catch (IOException e) {
            System.out.printf("[ERROR] Could not add mapping for document set \"%s\" to index \"%s\"...\n",
                    ESVarNames.INDEX_DOC_TYPE_NAME.getText(),
                    Config.getIndexName()
            );
            throw new RuntimeException("Error occurred while generating mapping for document type", e);
        }
    }

    /**
     *
     */
    private void openIndex() {
        OpenIndexResponse openIndexResponse = client.getClient().admin().indices().prepareOpen(Config.getIndexName()).get();
        if (openIndexResponse.isAcknowledged())
            System.out.println("[SUCCESS] "+ Config.getIndexName() + " is open");
    }

    /**
     *
     */
    private void closeIndex() {
        CloseIndexResponse closeIndexResponse = client.getClient().admin().indices().prepareClose(Config.getIndexName()).get();
        if (closeIndexResponse.isAcknowledged())
            System.out.println("[SUCCESS] "+ Config.getIndexName() + " is closed");
    }

    /**
     *
     * @param settings  settings object
     */
    public void updateIndexSettings(Map<String, Object> settings) {
        closeIndex();
        client.getClient().admin().indices().prepareUpdateSettings(Config.getIndexName()).setSettings(settings).get();
        openIndex();
    }

    /**
     *
     * @param analyzerName  name of he new analyzer to be added
     * @param analyzerType  the analyzer type - custom, standard etc.
     * @param tokenizer     type of tokenizer we will use
     * @param filter        type of term filter
     */
    public void addAnalyzer(String analyzerName, String analyzerType, String tokenizer, List<String> filter) {
        String settings = registerNewAnalyzer(analyzerName, analyzerType, tokenizer, filter);
        closeIndex();
        client.getClient().admin().indices().prepareUpdateSettings(Config.getIndexName()).setSettings(settings).get();
        openIndex();
        System.out.printf("[SUCCESS] Adding analyzer %s to %s/%s...\n", analyzerName, Config.getIndexName(), analyzerType);
    }

    /**
     *  This just waits for the cluster state to turn yellow
     *  so that further transactions can take place
     */
    public void waitForYellowState() {
        ClusterHealthResponse clusterHealthResponse = client.getClient().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        if(clusterHealthResponse.isTimedOut()) {
            System.out.println("There is some issue on connecting to the cluster");
        }
    }

    /**
     *
     * @param indexName
     * @param analyzer
     * @param tokenFilters
     * @param text
     * @return              tokens of analyzed text (Analyze API)
     */
    public List<String> analyzeText(String indexName, String analyzer, String[] tokenFilters, String text) {
        List<String> tokens = new ArrayList<String>();
        AnalyzeRequestBuilder analyzeRequestBuilder = client.getClient().admin().indices().prepareAnalyze(text);
        if (indexName != null) {
            analyzeRequestBuilder.setIndex(indexName);
        }
        if (analyzer != null) {
            analyzeRequestBuilder.setAnalyzer(analyzer);
        }
        if (tokenFilters != null) {
            analyzeRequestBuilder.setTokenFilters(tokenFilters);
        }
        logger.debug("Analyze request is text: {}, analyzer: {}, tokenfilters: {}",
                analyzeRequestBuilder.request().text(),
                analyzeRequestBuilder.request().analyzer(),
                analyzeRequestBuilder.request().tokenFilters());

        AnalyzeResponse analyzeResponse = analyzeRequestBuilder.get();
        try {
            if(analyzeResponse != null) {
                logger.debug("Analyze response is : {}",
                        analyzeResponse.toXContent(jsonBuilder().startObject(),
                                ToXContent.EMPTY_PARAMS).prettyPrint().string());
                for (AnalyzeResponse.AnalyzeToken analyzeToken : analyzeResponse.getTokens()) {
                    tokens.add(analyzeToken.getTerm());
                }
            }
        } catch (IOException e) {
            logger.error("Error printing response.", e);
        }
        return tokens;
    }

    /**
     *
     * @param MongOID
     * @throws Exception
     */
    public ImplementIndex(String MongOID) throws Exception {
        this.setIndexConf(MongOID);
        if (!this.isIndexExists()) {
            this.initIndex();
            this.waitForYellowState();
            this.refreshServer();
        }
    }

    public static void main(String[] args) throws Exception {
        String MONGO_ID = "37c8015d3777d422e7b637d93ce7567d";
        ImplementIndex newIndex = new ImplementIndex(MONGO_ID);

        List<String> filterList = Arrays.asList(
                ESKeywords.LOWERCASE.getText(),
                newIndex.Config.getStopWordFilterSmartName(),
                newIndex.Config.getStemmerFilterName()
        );

        newIndex.addAnalyzer(
                ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID,
                ESKeywords.CUSTOM.getText(),
                ESKeywords.WHITESPACE.getText(),
                filterList
        );

        newIndex.updateMapping(ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID, 0.99, 0.01);

        //newIndex.deleteTemplate(ESVarNames.TEMPLATE_NAME.getText());
        //newIndex.deleteIndex(indexName);
    }
}
