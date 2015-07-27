package main;

/**
 * Created by Animesh Pandey on 7/17/15.
 */

import org.consilience.helpers.ESKeywords;
import org.consilience.helpers.ESVarNames;
import org.consilience.persist.WordDocMatrix;
import org.consilience.service.ImplementIndex;
import org.consilience.persist.MongoIngestor;
import org.consilience.service.termvectors.TermVector;

import java.util.ArrayList;

public class Main {

    public static void main(String[] args) throws Exception {

        MongoIngestor ingestor = new MongoIngestor();
        ingestor.init("/home/cloudera/Documents/bbc/entertainment");
        ingestor.init("/home/cloudera/Documents/bbc/pdf_ent");

        String MONGO_ID = "5589b14f3004fb6be70e4724";
        ImplementIndex newIndex = new ImplementIndex(MONGO_ID);

        ArrayList<String> filterList = new ArrayList<>();
        filterList.add(ESKeywords.LOWERCASE.getText());
        filterList.add(newIndex.Config.getStopWordFilterSmartName());
        filterList.add(newIndex.Config.getPresOrigFilterName());
        filterList.add(newIndex.Config.getStemmerFilterName());
        filterList.add("asciifolding");
        filterList.add(newIndex.Config.getAposReplaceFilterName());

        newIndex.addAnalyzer(
                ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID,
                ESKeywords.CUSTOM.getText(),
                ESKeywords.STANDARD.getText(),
                filterList
        );

        newIndex.updateMapping(ESVarNames.ANALYZER_PREFIX.getText() + MONGO_ID, 0.99, 0.01);

        newIndex.bulkIndexFromMongo();

        newIndex.refreshServer();

        new TermVector().scanDocumentSet("classifier_5589b14f3004fb6be70e4724");

        new WordDocMatrix().readDataStore();
    }
}
