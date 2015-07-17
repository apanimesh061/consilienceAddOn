package main;

/**
 * Created by cloudera on 7/17/15.
 */

import org.consilience.helpers.ESKeywords;
import org.consilience.helpers.ESVarNames;
import org.consilience.indexer.Document;
import org.consilience.service.ImplementIndex;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        String MONGO_ID = "37c8015d3777d422e7b637d93ce7567d";
        ImplementIndex newIndex = new ImplementIndex(MONGO_ID);

        // initialize new analyzer
        List<String> filterList = Arrays.asList(
                ESKeywords.LOWERCASE.getText(),
                newIndex.Config.getStopWordFilterSmartName(),
                newIndex.Config.getStemmerFilterName()
        );

        // add the analyzer to the settings
        newIndex.addAnalyzer(
                ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID,
                ESKeywords.CUSTOM.getText(),
                ESKeywords.WHITESPACE.getText(),
                filterList
        );

        // add the analyzer and new term filter to mapping
        newIndex.updateMapping(ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID, 0.99, 0.01);

        newIndex.closeClient();

        // initialize new documents
        //Document d1 = new Document(0, "docset1", "docid1", "This is plain text retrieved from Mongo.");
        //Document d2 = new Document(1, "docset2", "docid2", "VGhpcyBpcyBwZGYgdGV4dCByZXRyaWV2ZWQgZnJvbSBNb25nby4=");
        // index the documents
        //d1.indexTo(newIndex.Config.getIndexName(), ESVarNames.INDEX_DOC_TYPE_NAME.getText());
        //d2.indexTo(newIndex.Config.getIndexName(), ESVarNames.INDEX_DOC_TYPE_NAME.getText());
    }
}
