package org.consilience.persist;

/**
 * Created by Animesh Pandey on 7/23/15.
 */

import org.consilience.persist.pojos.DocumentPojo;
import org.mongodb.morphia.Datastore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class MongoIngestor {
    public void init(String pathToFiles) throws IOException {
        File currDir = new File(pathToFiles);
        File[] list = currDir.listFiles();

        int docIndex = 0;

        Datastore datastore = DataStoreConnect.getDataStore("documents");

        assert list != null;
        for (File doc : list) {
            byte[] encoded = Files.readAllBytes(new File(doc.getAbsolutePath()).toPath());
            DocumentPojo documentPojo = new DocumentPojo(docIndex, doc.getName(), new String(encoded, "utf-8"));
            datastore.save(documentPojo);
            assert(documentPojo.equals(datastore.get(DocumentPojo.class, documentPojo.getId())));

            System.out.printf("Document [%s] ingested in Mongo...\n", doc.getName());

            docIndex ++;
        }
    }
}
