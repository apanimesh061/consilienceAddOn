package org.consilience.helpers;

/**
 * Created by cloudera on 7/10/15.
 */

public enum ESVarNames {

    ADDRESS("localhost"),
    TCP_PORT("9300"),
    HTTP_PORT("9200"),

    CLUSTER_NAME("elasticsearch"),

    DOCSET_ID("docset_id"),
    DOC_ID("doc_id"),
    PLAIN_TEXT("plain_text"),
    PDF_TEXT("pdf_text"),

    INDEX_DOC_TYPE_NAME("document_set"),

    TEMPLATE_NAME("classifier_template"),
    TEMPLATE_INDEX_PATTERN("classifier_*"),
    INDEX_PREFIX("classifier_"),
    ANALYZER_PREFIX("analyzer_"),

    MIN_SHINGLE_VALUE("2"),
    MAX_SHINGLE_VALUE("2")

    ;

    private String text;

    public String getText() {
        return text;
    }

    ESVarNames(String word) {
        this.text = word;
    }
}
