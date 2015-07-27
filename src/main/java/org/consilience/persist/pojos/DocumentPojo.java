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

    private Integer documentType;
    private String documentSetId;
    private String docid;
    private Integer index;
    private String text;

    public DocumentPojo(Integer documentType, String documentSetId, String docid, Integer index, String text) {
        this.documentType = documentType;
        this.documentSetId = documentSetId;
        this.docid = docid;
        this.index = index;
        this.text = text;
    }

    public DocumentPojo() {}

    public String getDocid() {
        return docid;
    }

    public void setDocid(String docid) {
        this.docid = docid;
    }

    public String getDocumentSetId() {
        return documentSetId;
    }

    public void setDocumentSetId(String documentSetId) {
        this.documentSetId = documentSetId;
    }

    public Integer getDocumentType() {
        return documentType;
    }

    public void setDocumentType(Integer documentType) {
        this.documentType = documentType;
    }

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
