package org.consilience.service.termvectors;

import com.google.gson.*;

import org.consilience.helpers.ESVarNames;
import org.consilience.indexer.StartTCPService;
import org.consilience.persist.ToNGram;
import org.consilience.persist.TermDataPojo;

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

/**
 * Created by cloudera on 7/17/15.
 */
public class TermVector {
    private static final Logger logger = LoggerFactory.getLogger(TermVector.class);

    private StartTCPService client = new StartTCPService();

    private static long termCount = 0;

    private static Map<String, List<Long>> vocabulary = new HashMap<>();

    private QueryBuilder Query() {
        return QueryBuilders.matchAllQuery();
    }

    public void scanDocumentSet(String indexName) throws IOException {
        SearchRequestBuilder searchRequestBuilder = client.getClient().prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(Query())
                .setSize(10);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        Datastore datastore = ToNGram.getDataStore("ngrams");

        while(true) {
            searchResponse = client.getClient()
                    .prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000)).execute().actionGet();
            for (SearchHit hit : searchResponse.getHits()) {
                Map<String, Map<String, Long>> termVector = getTermVector(indexName, "document_set", hit.getId());
                termVector.keySet().forEach(term -> {
                    long termFreq = termVector.get(term).get("term_freq");
                    List<Long> termData = Arrays.asList(termCount, termFreq);
                    if (!vocabulary.containsKey(term)) {
                        vocabulary.put(term, termData);

                        TermDataPojo pojo = new TermDataPojo(term, termCount, termFreq);
                        datastore.save(pojo);
                        assert(pojo.equals(datastore.get(TermDataPojo.class, pojo.getId())));

                        //System.out.printf("[%s]\t[%d]\t[%d]\n", term, termCount, termFreq);
                    }
                    termCount ++;
                });
                termVector.clear();
            }
            if (searchResponse.getHits().hits().length == 0)
                break;
        }
    }

    public Map<String, Map<String, Long>> getTermVector(String indexName, String documentType, String documentID) throws IOException {
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

        Map<String, Map<String, Long>> termStatsMapping = new HashMap<>();

        for (String s : termVectorResponse.getFields()) {
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(Json)
                    .getAsJsonObject().get("term_vectors")
                    .getAsJsonObject().get(s)
                    .getAsJsonObject().get("terms")
                    .getAsJsonObject();
            Set<Map.Entry<String,JsonElement>> entrySet = jsonObject.entrySet();

            for (Map.Entry<String, JsonElement> stringJsonElementEntry : entrySet) {
                Gson gson = new GsonBuilder().create();
                Term term = gson.fromJson(stringJsonElementEntry.getValue().getAsJsonObject(), Term.class);

                Map<String, Long> currentTermStats = new HashMap<>();
                currentTermStats.put("doc_freq", term.getDoc_freq());
                currentTermStats.put("ttf", term.getTtf());
                currentTermStats.put("term_freq", term.getTerm_freq());

                termStatsMapping.put(stringJsonElementEntry.getKey(), currentTermStats);
            }
        }
        return termStatsMapping;
    }

    public static void main(String[] args) throws IOException {
        TermVector termVector = new TermVector();
        termVector.scanDocumentSet("classifier_37c8015d3777d422e7b637d93ce7567d");
    }
}

class Term {
    private long doc_freq;
    private long ttf;
    private long term_freq;

    public Term(long doc_freq, long ttf, long term_freq) {
        this.doc_freq = doc_freq;
        this.ttf = ttf;
        this.term_freq = term_freq;
    }

    public Term() {
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
