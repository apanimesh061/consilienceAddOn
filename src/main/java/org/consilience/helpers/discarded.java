package org.consilience.helpers;

/**
 * Created by Animesh Pandey on 7/21/15.
 */
public class discarded {
    /*
    public void scanDocumentSet1(String indexName) throws IOException {
        String MONGO_ID = "37c8015d3777d422e7b637d93ce7567d";
        SearchRequestBuilder searchRequestBuilder = client.getClient().prepareSearch(indexName)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryStem("expect"))
                .setSize(10);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        while(true) {
            searchResponse = client.getClient()
                    .prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            for (SearchHit hit : searchResponse.getHits()) {
                Map<String, Object> fullResult = hit.getSource();
                fullResult.keySet().forEach(key -> {
                    if (fullResult.containsKey(ESVarNames.PLAIN_TEXT.getText())) {
                        //System.out.println(fullResult.get(ESVarNames.PLAIN_TEXT.getText()));
                        List<String> standard = analyzeText(
                                indexName,
                                ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID,
                                null,
                                null,
                                (String) fullResult.get(ESVarNames.PLAIN_TEXT.getText())
                        );
                        standard.forEach(
                                t -> {
                                    List<String> unstemmed = analyzeText(
                                            indexName,
                                            ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID + "_other",
                                            null, null, t
                                    );
                                    if (unstemmed.size() > 0) {
                                        System.out.println(unstemmed.get(0) + " " + t);
                                    }
                                }
                        );
                    } else
                        System.out.println(fullResult.get(ESVarNames.PDF_TEXT.getText()));
                });
                fullResult.clear();
            }
            if (searchResponse.getHits().hits().length == 0)
                break;
        }
    }

    public void scanDocumentSet(String indexName) throws IOException {
        SearchRequestBuilder searchRequestBuilder = client.getClient().prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(Query())
                .setSize(10);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        Datastore datastore = DataStoreConnect.getDataStore("ngrams");

        while(true) {
            searchResponse = client.getClient()
                    .prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            for (SearchHit hit : searchResponse.getHits()) {
                Map<String, Map<String, Object>> termVector = getTermVector(indexName, "document_set", hit.getId());
                termVector.keySet().forEach(term -> {
                    Long termFreq = (Long) termVector.get(term).get("term_freq");
                    List<Long> termData = Arrays.asList(termCount, termFreq);
                    if (!vocabulary.containsKey(term)) {
                        vocabulary.put(term, termData);

                        //TermDataPojo pojo = new TermDataPojo(term, termCount, termFreq);
                        //datastore.save(pojo);
                        //assert(pojo.equals(datastore.get(TermDataPojo.class, pojo.getId())));

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
    */
}
