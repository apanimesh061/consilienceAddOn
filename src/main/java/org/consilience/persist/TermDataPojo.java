package org.consilience.persist;

/**
 * Created by Animesh Pandey on 7/19/15.
 */

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

@Entity(value = "ngrams", noClassnameStored = true)
public class TermDataPojo {
    private String term;
    @Id
    private long id;
    private long termCount;

    public TermDataPojo(String term, long id, long termCount) {
        this.term = term;
        this.id = id;
        this.termCount = termCount;
    }

    public TermDataPojo() {

    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getTermCount() {
        return termCount;
    }

    public void setTermCount(long termCount) {
        this.termCount = termCount;
    }
}
