package org.consilience.persist;

/**
 * Created by Animesh Pandey on 7/21/15.
 */


import org.consilience.persist.pojos.DocumentPojo;
import org.consilience.persist.pojos.TermDataPojo;
import org.mongodb.morphia.query.Query;

import org.consilience.helpers.ESVarNames;
import org.consilience.service.termvectors.TermVector;
import org.consilience.persist.pojos.SparseMatrix;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import cern.colt.list.LongArrayList;

public class WordDocMatrix {

    private long getNgramIndex(String term) {
        Query query = DataStoreConnect.ngram_datastore.find(TermDataPojo.class, "stem", term);
        try {
            TermDataPojo termDataPojos = (TermDataPojo) query.asList().get(0);
            return termDataPojos.getNgramIndex();
        } catch (Exception e) {
            return -1;
        }
    }

    private Map<String, Long> getAllTermsFromVector(String docID) throws IOException {
        TermVector termVector = new TermVector();
        Map<String, Map<String, Object>> t = termVector.getTermVector(
                "classifier_5589b14f3004fb6be70e4724",
                ESVarNames.INDEX_DOC_TYPE_NAME.getText(),
                docID
        );

        return t.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> (Long) e.getValue().get("term_freq")
                ));
    }

    public void readDataStore() throws IOException {
        Query query = DataStoreConnect.document_datastore.find(DocumentPojo.class);
        List<DocumentPojo> documentPojos = query.asList();
        LongArrayList stemArrayList = new LongArrayList();
        LongArrayList countArrayList = new LongArrayList();

        documentPojos.forEach(obj -> {
                    try {
                        SparseMatrix sparseMatrix = new SparseMatrix();

                        sparseMatrix.setDocumentID(obj.getIndex());

                        Map<String, Long> temp = getAllTermsFromVector(obj.getDocid());
                        temp.forEach((term, count) -> {
                                    long currIndex = getNgramIndex(term);
                                    if (currIndex > 0) {
                                        stemArrayList.add(currIndex);
                                        countArrayList.add(count);
                                    }
                                }
                        );

                        ByteBuffer byteBuffer = ByteBuffer.allocate(stemArrayList.size() * 8);

                        for(int i = 0; i < stemArrayList.size(); ++i)
                            byteBuffer.putLong(stemArrayList.get(i));
                        sparseMatrix.setStemIndexBinary(byteBuffer);
                        byteBuffer.clear();

                        for(int i = 0; i < countArrayList.size(); ++i)
                            byteBuffer.putLong(countArrayList.get(i));
                        sparseMatrix.setStemCountBinary(byteBuffer);
                        byteBuffer.clear();

                        DataStoreConnect.wdm_datastore.save(sparseMatrix);
                        assert(sparseMatrix.equals(DataStoreConnect.wdm_datastore.get(SparseMatrix.class, sparseMatrix.getId())));

                        System.out.printf("Row [%s] added to SparseMatrix successfully...\n", obj.getIndex());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    public static void main(String[] args) throws IOException {
        new WordDocMatrix().readDataStore();
    }
}
