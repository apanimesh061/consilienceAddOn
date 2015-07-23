package org.consilience.persist;


import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

public class DataStoreConnect {
    public static Datastore document_datastore = getDataStore("documents");
    public static Datastore ngram_datastore = getDataStore("ngrams");
    public static Datastore wdm_datastore = getDataStore("wordDocMatrix");

    public static Datastore getDataStore(String dbName) {
        MongoClient mongoClient = new com.mongodb.MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        return new Morphia().createDatastore(mongoClient, dbName);
    }
}
