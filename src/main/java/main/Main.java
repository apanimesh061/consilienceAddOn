package main;

/**
 * Created by cloudera on 7/17/15.
 */

import org.consilience.helpers.ESKeywords;
import org.consilience.helpers.ESVarNames;
import org.consilience.persist.WordDocMatrix;
import org.consilience.service.ImplementIndex;
import org.consilience.persist.MongoIngestor;
import org.consilience.service.termvectors.TermVector;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {

//        new MongoIngestor().init("/home/cloudera/Documents/bbc/entertainment");
//
//        String MONGO_ID = "5589b14f3004fb6be70e4724";
//        ImplementIndex newIndex = new ImplementIndex(MONGO_ID);
//
//        List<String> filterList = Arrays.asList(
//                ESKeywords.LOWERCASE.getText(),
//                newIndex.Config.getStopWordFilterSmartName(),
//                newIndex.Config.getStemmerFilterName(),
//                newIndex.Config.getWordDelimFilterName(),
//                newIndex.Config.getPresOrigFilterName()
//        );
//
//        newIndex.addAnalyzer(
//                ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID,
//                ESKeywords.CUSTOM.getText(),
//                ESKeywords.STANDARD.getText(),
//                filterList
//        );
//
//        newIndex.updateMapping(ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID, 0.99, 0.01);
//
//        newIndex.bulkIndexFromMongo();
//
//        newIndex.refreshServer();

        //new TermVector().scanDocumentSet("classifier_5589b14f3004fb6be70e4724");

        new WordDocMatrix().readDataStore();
    }
}
