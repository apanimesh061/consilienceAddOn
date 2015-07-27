package org.consilience.persist;

/**
 * Created by Animesh Pandey on 7/23/15.
 */

import org.consilience.persist.pojos.DocumentPojo;

import org.mongodb.morphia.Datastore;

//import org.elasticsearch.common.Base64;
import java.util.Base64;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MongoIngestor {
    private static int docIndex = 0;

    public void init(String pathToFiles) throws IOException {
        File currDir = new File(pathToFiles);
        File[] list = currDir.listFiles();

        Datastore datastore = DataStoreConnect.document_datastore;

        assert list != null;
        for (File doc : list) {
            Path filePath = new File(doc.getAbsolutePath()).toPath();
            String mimeType = Files.probeContentType(filePath);

            byte[] source = Files.readAllBytes(filePath);

            final String content = (mimeType.equalsIgnoreCase("text/plain")) ? new String(source, "utf-8") : new String(Base64.getEncoder().encode(source), "utf-8");
            final Integer docType = (mimeType.equalsIgnoreCase("text/plain")) ? 0 : 1;

            DocumentPojo documentPojo = new DocumentPojo(docType, "document_set_id" , doc.getName(), docIndex, content);
            datastore.save(documentPojo);
            assert(documentPojo.equals(datastore.get(DocumentPojo.class, documentPojo.getId())));

            System.out.printf("Document [%s] ingested in Mongo...\n", doc.getName());

            docIndex ++;
        }
    }

    public static void main(String[] args) throws IOException {
        MongoIngestor ingestor = new MongoIngestor();
        ingestor.init("/home/cloudera/Documents/bbc/entertainment");
        ingestor.init("/home/cloudera/Documents/bbc/pdf_ent");
    }
}
