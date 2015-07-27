package org.consilience.service;

/**
 * Created by cloudera on 7/13/15.
 */

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import org.consilience.helpers.ESKeywords;
import org.consilience.helpers.ESVarNames;
import org.consilience.indexer.Document;
import org.consilience.persist.pojos.DocumentPojo;
import org.consilience.persist.DataStoreConnect;
import org.consilience.service.setup.SetupService;
import org.consilience.indexer.StartTCPService;
import org.consilience.conf.IndexConfig;
import org.consilience.conf.IndexConfig.SupportedLocale;
import org.consilience.service.setup.templates.Template;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.Query;
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
        //Config.setIndexName(indexName);
        //Config.setSupportedLocale(locale);
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
//                        .startObject(ESKeywords.CHAR_FILTER.getText())
//                            .startObject("apos_remove")
//                                .field(ESKeywords.TYPE.getText(), ESKeywords.MAPPING.getText())
//                                .field(ESKeywords.MAPPINGS.getText(), Arrays.asList("`t=>", "'t=>"))
//                            .endObject()
//                        .endObject()
                        .startObject(ESKeywords.ANALYZER.getText())
                            .startObject(analyzerName)
                                .field(ESKeywords.TYPE.getText(), analyzerType)
                                .field(ESKeywords.TOKENIZER.getText(), tokenizer)
                                .field(ESKeywords.FILTER.getText(), filter)
                                //.field(ESKeywords.CHAR_FILTER.getText(), Arrays.asList("apos_remove"))
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
     * @param MongOID
     * @throws Exception
     */
    public ImplementIndex(String MongOID) throws Exception {
        this.setIndexConf(MongOID); // initialises new conf for new ID
        if (!this.isIndexExists()) {
            this.initIndex();
            this.waitForYellowState();
            this.refreshServer();
        }
    }

    /**
     *
     */
    public void closeClient() {
        client.getClient().close();
    }

    /**
     *
     * @return
     */
    private BulkProcessor bulkIndexerInit() {
        return BulkProcessor.builder(
                client.getClient(),
                new BulkProcessor.Listener() {
                    public void beforeBulk(long executionID, BulkRequest request) {
                        logger.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
                    }

                    public void afterBulk(long executionID, BulkRequest request, BulkResponse response) {
                        logger.info("Executed bulk composed of {} actions", request.numberOfActions());
                    }

                    public void afterBulk(long executionID, BulkRequest request, Throwable failure) {
                        logger.warn("Error executing bulk", failure);
                    }
                })
                .setBulkActions(20)
                .setBulkSize(new ByteSizeValue(4, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(10))
                .setConcurrentRequests(1)
                .build();
    }

    public void bulkIndexFromMongo() throws IOException {
        BulkProcessor bulkProcessor = bulkIndexerInit();
        Datastore datastore = DataStoreConnect.document_datastore;
        final Query query  = datastore.createQuery(DocumentPojo.class);
        List<DocumentPojo> answers = query.asList();
        answers.forEach(ans -> {
                    Document currDoc = new Document(ans.getDocumentType(), ans.getDocumentSetId(), ans.getDocid(), ans.getText());
                    try {
                        currDoc.setJson();
                        bulkProcessor.add(
                                new IndexRequest(
                                        Config.getIndexName(),
                                        ESVarNames.INDEX_DOC_TYPE_NAME.getText(),
                                        ans.getDocid()
                                ).source(currDoc.getJson())
                        );
                        System.out.printf("Document [%s\\%s\\%s] was successfully indexed\n",
                                Config.getIndexName(),
                                ESVarNames.INDEX_DOC_TYPE_NAME.getText(),
                                ans.getDocid()
                        );
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
        bulkProcessor.close();
    }

    public void bulkIndexer(String path) throws Exception {
        File file  = new File(path);
        String[] dirs = file.list((dir, name) -> new File(dir, name).isDirectory());

        BulkProcessor bulkProcessor = bulkIndexerInit();

        for (String dir : dirs) {
            File currDir = new File(path + dir);
            File[] list = currDir.listFiles();

            assert list != null;
            for (File doc : list) {
                byte[] encoded = Files.readAllBytes(new File(doc.getAbsolutePath()).toPath());
                Document currDoc = new Document(0, currDir.getName(), doc.getName(), new String(encoded, "utf-8"));
                currDoc.setJson();
                bulkProcessor.add(
                        new IndexRequest(
                                Config.getIndexName(),
                                ESVarNames.INDEX_DOC_TYPE_NAME.getText(),
                                doc.getName()
                        ).source(currDoc.getJson())
                );
                System.out.printf("Document [%s\\%s\\%s] was successfully indexed\n",
                        Config.getIndexName(),
                        ESVarNames.INDEX_DOC_TYPE_NAME.getText(),
                        doc.getName()
                );
            }
        }
        bulkProcessor.close();
    }

    public static void main(String[] args) throws Exception {
        String MONGO_ID = "5589b14f3004fb6be70e4724";
        ImplementIndex newIndex = new ImplementIndex(MONGO_ID);
;
        ArrayList<String> filterList = new ArrayList<>();
        filterList.add(ESKeywords.LOWERCASE.getText());
        filterList.add(newIndex.Config.getStopWordFilterSmartName());
        filterList.add(newIndex.Config.getPresOrigFilterName());
        filterList.add(newIndex.Config.getStemmerFilterName());
        filterList.add(newIndex.Config.getAposReplaceFilterName());

        newIndex.addAnalyzer(
                ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID,
                ESKeywords.CUSTOM.getText(),
                ESKeywords.STANDARD.getText(),
                filterList
        );

        newIndex.updateMapping(ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID, 0.99, 0.01);

        //newIndex.bulkIndexer("/home/cloudera/Documents/bbc");
        newIndex.bulkIndexFromMongo();

        newIndex.refreshServer();

        //newIndex.deleteTemplate(ESVarNames.TEMPLATE_NAME.getText());
        //newIndex.deleteIndex(indexName);
    }
}
