package org.consilience.persist.pojos;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.nio.ByteBuffer;

/**
 * Created by Animesh Pandey on 7/23/15.
 */

@Entity(value = "wordDocMatrix", noClassnameStored = true)
public class SparseMatrix {
    @Id
    ObjectId id;

    private long documentID;
    private ByteBuffer stemIndexBinary;
    private ByteBuffer stemCountBinary;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public long getDocumentID() {
        return documentID;
    }

    public void setDocumentID(long documentID) {
        this.documentID = documentID;
    }

    public ByteBuffer getStemIndexBinary() {
        return stemIndexBinary;
    }

    public void setStemIndexBinary(ByteBuffer stemIndexBinary) {
        this.stemIndexBinary = stemIndexBinary;
    }

    public ByteBuffer getStemCountBinary() {
        return stemCountBinary;
    }

    public void setStemCountBinary(ByteBuffer stemCountBinary) {
        this.stemCountBinary = stemCountBinary;
    }

    public SparseMatrix(long documentID, ByteBuffer stemIndexBinary, ByteBuffer stemCountBinary) {
        this.documentID = documentID;
        this.stemIndexBinary = stemIndexBinary;
        this.stemCountBinary = stemCountBinary;
    }

    public SparseMatrix() {}
}
