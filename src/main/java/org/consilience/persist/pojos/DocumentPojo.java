package org.consilience.persist.pojos;

/**
 * Created by Animesh Pandey on 7/21/15.
 */

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

@Entity(value = "documents", noClassnameStored = true)
public class DocumentPojo {
    @Id
    ObjectId id;

    private Integer index;
    private String docid;
    private String text;

    public String getDocid() {
        return docid;
    }

    public void setDocid(String docid) {
        this.docid = docid;
    }

    public DocumentPojo(Integer index, String docid, String text) {
        this.index = index;
        this.text = text;
        this.docid = docid;
    }

    public DocumentPojo() {}

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
