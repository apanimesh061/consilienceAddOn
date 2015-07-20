package org.consilience.persist;


import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

public class ToNGram {
    public static Datastore getDataStore(String dbName) {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        return new Morphia().createDatastore(mongoClient, dbName);
    }
}
