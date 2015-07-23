package org.consilience.service.termvectors;

/**
 * Created by Animesh Pandey on 7/17/15.
 */

import com.google.gson.*;

import org.bson.types.ObjectId;

import org.consilience.helpers.ESVarNames;
import org.consilience.helpers.Tuple2;
import org.consilience.indexer.StartTCPService;
import org.consilience.persist.DataStoreConnect;
import org.consilience.persist.pojos.TermDataPojo;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import org.mongodb.morphia.Datastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class TokenPositions {
    private long position;
    private Integer start_offset;
    private Integer end_offset;

    public TokenPositions() {}

    public TokenPositions(long position, Integer start_offset, Integer end_offset) {
        this.position = position;
        this.start_offset = start_offset;
        this.end_offset = end_offset;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public Integer getStart_offset() {
        return start_offset;
    }

    public void setStart_offset(Integer start_offset) {
        this.start_offset = start_offset;
    }

    public Integer getEnd_offset() {
        return end_offset;
    }

    public void setEnd_offset(Integer end_offset) {
        this.end_offset = end_offset;
    }
}

class Term {
    private long doc_freq;
    private long ttf;
    private long term_freq;
    private List<TokenPositions> tokens;

    public Term(long doc_freq, long ttf, long term_freq, List<TokenPositions> tokens) {
        this.doc_freq = doc_freq;
        this.ttf = ttf;
        this.term_freq = term_freq;
        this.tokens = tokens;
    }

    public Term() {}

    public List<TokenPositions> getTokens() {
        return tokens;
    }

    public void setTokens(List<TokenPositions> tokens) {
        this.tokens = tokens;
    }

    public long getDoc_freq() {
        return doc_freq;
    }

    public void setDoc_freq(long doc_freq) {
        this.doc_freq = doc_freq;
    }

    public long getTtf() {
        return ttf;
    }

    public void setTtf(long ttf) {
        this.ttf = ttf;
    }

    public long getTerm_freq() {
        return term_freq;
    }

    public void setTerm_freq(long term_freq) {
        this.term_freq = term_freq;
    }
}

public class TermVector {
    private static final Logger logger = LoggerFactory.getLogger(TermVector.class);

    private StartTCPService client = new StartTCPService();

    private static long termCount = 0;

    private static Map<String, List<Long>> vocabulary = new HashMap<>();

    private QueryBuilder Query() {
        return QueryBuilders.matchAllQuery();
    }

    private QueryBuilder QueryStem(String stem) {
        return QueryBuilders.multiMatchQuery(
                stem,
                ESVarNames.PLAIN_TEXT.getText(),
                ESVarNames.PDF_TEXT.getText()
        );
    }

    private Map<String, Integer> processSearchResults(
                Map<String, Integer> unstemmed,
                SearchResponse searchResponse,
                String indexName,
                String stem) throws IOException {

        for (SearchHit hit : searchResponse.getHits()) {
            Map<String, Object> fullResult = hit.getSource();
            //System.out.println(stem + " " + fullResult.get("doc_id"));
            Map<String, Map<String, Object>> termVector =
                    getTermVector(indexName, ESVarNames.INDEX_DOC_TYPE_NAME.getText(), (String) fullResult.get(ESVarNames.DOC_ID.getText()));

            try {
                List<Tuple2<Integer, Integer>> offsets = (List<Tuple2<Integer, Integer>>) termVector.get(stem).get("tokens");
                if (fullResult.containsKey(ESVarNames.PLAIN_TEXT.getText())) {
                    String currentText = (String) fullResult.get(ESVarNames.PLAIN_TEXT.getText());
                    offsets.forEach(
                            t -> {
                                String currentUnstemmedTerm = currentText.substring(t._0, t._1);
                                Integer count = unstemmed.get(currentUnstemmedTerm);
                                unstemmed.put(currentUnstemmedTerm, (count == null) ? 1 : count + 1);
                            }
                    );
                } else {
                    String currentText = (String) fullResult.get(ESVarNames.PDF_TEXT.getText());
                    offsets.forEach(
                            t -> {
                                String currentUnstemmedTerm = currentText.substring(t._0, t._1);
                                Integer count = unstemmed.get(currentUnstemmedTerm);
                                unstemmed.put(
                                        currentUnstemmedTerm,
                                        (count == null) ? 1 : count + 1
                                );
                            }
                    );
                }
                return unstemmed;
            } catch (NullPointerException ex) {
                System.out.println("Not found in term-vector : " + stem);
            }
        }
        return unstemmed;
    }

    public Tuple2<Set<String>, String> getUnstemmedTerms(String indexName, String stem) throws IOException {
        Map<String, Integer> unstemmed = new HashMap<>();
        SearchRequestBuilder searchRequestBuilder = client.getClient().prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryStem(stem))
                .setFrom(1)
                .setSize(1);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        unstemmed = processSearchResults(unstemmed, searchResponse, indexName, stem);

        while(true) {
            searchResponse = client.getClient()
                    .prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();

            unstemmed = processSearchResults(unstemmed, searchResponse, indexName, stem);

            if (searchResponse.getHits().hits().length == 0)
                break;
        }
        try {
            String text = Collections.max(unstemmed.entrySet(), (entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1).getKey();
            //System.out.println(text + " " + unstemmed.keySet().toString());
            return new Tuple2<>(unstemmed.keySet(), text);
        } catch (NoSuchElementException ex) {
            System.out.println("------------------------------------------ " + stem);
            Set<String> h = new HashSet<>(Arrays.asList(stem));
            return new Tuple2<>(h, stem);
        }
    }

    public Map<String, Map<String, Object>> getTermVector(String indexName, String documentType, String documentID) throws IOException {
        TermVectorRequest termVectorRequest = new TermVectorRequest()
                .index(indexName).type(documentType).id(documentID)
                .termStatistics(true).fieldStatistics(true)
                .selectedFields(ESVarNames.PLAIN_TEXT.getText(), ESVarNames.PDF_TEXT.getText());
        TermVectorResponse termVectorResponse = client.getClient().termVector(termVectorRequest).actionGet();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        termVectorResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String Json = builder.prettyPrint().string();

        Map<String, Map<String, Object>> termStatsMapping = new HashMap<>();

        for (String s : termVectorResponse.getFields()) {
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(Json)
                    .getAsJsonObject().get("term_vectors")
                    .getAsJsonObject().get(s)
                    .getAsJsonObject().get("terms")
                    .getAsJsonObject();
            Set<Map.Entry<String, JsonElement>> entrySet = jsonObject.entrySet();

            for (Map.Entry<String, JsonElement> stringJsonElementEntry : entrySet) {
                Gson gson = new GsonBuilder().create();
                Term term = gson.fromJson(stringJsonElementEntry.getValue().getAsJsonObject(), Term.class);

                List<Tuple2<Integer, Integer>> offsets = new ArrayList<>();

                term.getTokens().forEach(position ->
                                offsets.add(new Tuple2<>(position.getStart_offset(), position.getEnd_offset()))
                );

                Map<String, Object> currentTermStats = new HashMap<>();
                currentTermStats.put("doc_freq", term.getDoc_freq());
                currentTermStats.put("ttf", term.getTtf());
                currentTermStats.put("term_freq", term.getTerm_freq());
                currentTermStats.put("tokens", offsets);

                termStatsMapping.put(stringJsonElementEntry.getKey(), currentTermStats);
            }
        }
        return termStatsMapping;
    }

    public void scanDocumentSet(String indexName) throws IOException {
        String ClassifierID = indexName.substring(indexName.indexOf("_") + 1, indexName.length());
        SearchRequestBuilder searchRequestBuilder = client.getClient().prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(Query())
                .setSize(10);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        Datastore datastore = DataStoreConnect.ngram_datastore;

        while(true) {
            searchResponse = client.getClient()
                    .prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            for (SearchHit hit : searchResponse.getHits()) {
                Map<String, Map<String, Object>> termVector = getTermVector(indexName, ESVarNames.INDEX_DOC_TYPE_NAME.getText(), hit.getId());
                termVector.keySet().forEach(term -> {
                    Long termFreq = (Long) termVector.get(term).get("ttf");
                    List<Long> termData = Arrays.asList(termCount, termFreq);
                    if (!vocabulary.containsKey(term)) {
                        vocabulary.put(term, termData);
                        try {
                            Tuple2<Set<String>, String> importantUnstemmedTerms = getUnstemmedTerms(indexName, term);
                            Set<String> unstemmedTerms = importantUnstemmedTerms._0;
                            String unstemmedMaxFreq = importantUnstemmedTerms._1;
                            TermDataPojo pojo = new TermDataPojo(
                                    new ObjectId(ClassifierID),
                                    termCount,
                                    unstemmedMaxFreq,
                                    termFreq,
                                    term,
                                    unstemmedTerms
                            );
//                            System.out.printf("%s\t%d\t%s\t%d\t%s\t%s\n",
//                                    new ObjectId(ClassifierID),
//                                    termCount,
//                                    term,
//                                    termFreq,
//                                    unstemmedMaxFreq,
//                                    unstemmedTerms
//                            );
                            datastore.save(pojo);
                            assert(pojo.equals(datastore.get(TermDataPojo.class, pojo.getId())));

                            System.out.printf("Term [%s] added to the ngram collection successfully...\n", term);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    termCount ++;
                });
                termVector.clear();
            }
            if (searchResponse.getHits().hits().length == 0)
                break;
        }
    }

   public List<String> analyzeText(String indexName, String analyzer, String[] tokenFilters, String tokenizer, String text) {
        List<String> tokens = new ArrayList<>();
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
        if (tokenizer != null) {
            analyzeRequestBuilder.setTokenizer(tokenizer);
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
                tokens.addAll(analyzeResponse
                                .getTokens()
                                .stream()
                                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                        .collect(Collectors.toList())
                );
            }
        } catch (IOException e) {
            logger.error("Error printing response.", e);
        }
        return tokens;
    }

    public static void main(String[] args) throws IOException {
        TermVector termVector = new TermVector();
        //termVector.getUnstemmedTerms("classifier_5589b14f3004fb6be70e4724", "earli");
        termVector.scanDocumentSet("classifier_5589b14f3004fb6be70e4724");
        //termVector.getTermVector("classifier_37c8015d3777d422e7b637d93ce7567d", "document_set", "235.txt")
    }
}
