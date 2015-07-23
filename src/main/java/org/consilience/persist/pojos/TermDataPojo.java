package org.consilience.persist.pojos;

/**
 * Created by Animesh Pandey on 7/19/15.
 */

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.Set;

@Entity(value = "ngrams", noClassnameStored = true)
public class TermDataPojo {
    @Id
    ObjectId id;

    ObjectId classifierId;
    private long ngramIndex;
    private String text;
    private long count;
    private String stem;
    private Set<String> unstemmed;

    public TermDataPojo(ObjectId classifierId, long ngramIndex, String text, long count, String stem, Set<String> unstemmed) {
        this.classifierId = classifierId;
        this.ngramIndex = ngramIndex;
        this.text = text;
        this.count = count;
        this.stem = stem;
        this.unstemmed = unstemmed;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public ObjectId getClassifierId() {
        return classifierId;
    }

    public void setClassifierId(ObjectId classifierId) {
        this.classifierId = classifierId;
    }

    public long getNgramIndex() {
        return ngramIndex;
    }

    public void setNgramIndex(long ngramIndex) {
        this.ngramIndex = ngramIndex;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getStem() {
        return stem;
    }

    public void setStem(String stem) {
        this.stem = stem;
    }

    public Set<String> getUnstemmed() {
        return unstemmed;
    }

    public void setUnstemmed(Set<String> unstemmed) {
        this.unstemmed = unstemmed;
    }

    public TermDataPojo() {}
}
