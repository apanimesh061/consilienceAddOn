package org.consilience.indexer;

import org.consilience.helpers.ESVarNames;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.XContentBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by cloudera on 7/15/15.
 */

public class Document {
    private static final Logger logger = LoggerFactory.getLogger(Document.class);

    private StartTCPService client = new StartTCPService();

    // 0 means plain/text
    // 1 means application/pdf
    private int docType;
    private String documentSetID;
    private String documentID;
    private String content;
    private XContentBuilder json;

    public Document(int docType, String documentSetID, String documentID, String content) {
        this.docType = docType;
        this.documentSetID = documentSetID;
        this.documentID = documentID;
        this.content = content;
    }

    public XContentBuilder getJson() {
        return json;
    }

    public void setJson() throws IOException {
        if(getDocType() == 0)
            this.json = createPlainTextDocument(this.documentSetID, this.documentID, this.content);
        else
            this.json = createPDFDocument(this.documentSetID, this.documentID, this.content);
    }

    private String getDocumentID() {
        return documentID;
    }

    private void setDocumentID(String documentID) {
        this.documentID = documentID;
    }

    private String getDocumentSetID() {
        return documentSetID;
    }

    private void setDocumentSetID(String documentSetID) {
        this.documentSetID = documentSetID;
    }

    private String getContent() {
        return content;
    }

    private void setContent(String content) {
        this.content = content;
    }

    private int getDocType() {
        return docType;
    }

    private void setDocType(int docType) {
        this.docType = docType;
    }

    private String encode(byte[] data) throws IOException{
        return Base64.encodeBytes(data);
    }

    private String readPlainText (File file) throws IOException {
        byte[] encoded = Files.readAllBytes(file.toPath());
        return new String(encoded, "utf-8");
    }

    /**
     *
     * @param documentSetID     document set's ID
     * @param documentID        ID of current document in documentSetID
     * @param plainText         Plain UTF-8 text from a database
     * @return                  All data formatted to XContent compatible
     *                          with Index API
     */
    private XContentBuilder createPlainTextDocument(String documentSetID, String documentID, String plainText) throws IOException {
        return jsonBuilder()
                .startObject()
                    .field(ESVarNames.DOCSET_ID.getText(), documentSetID)
                    .field(ESVarNames.DOC_ID.getText(), documentID)
                    .field(ESVarNames.PLAIN_TEXT.getText(), plainText)
                .endObject();
    }

    /**
     *
     * @param documentSetID     document set's ID
     * @param documentID        ID of current document in documentSetID
     * @param encodedText       Base64 encoding from database
     * @return
     */
    private XContentBuilder createPDFDocument(String documentSetID, String documentID, String encodedText) throws IOException {
        return jsonBuilder()
                .startObject()
                    .field(ESVarNames.DOCSET_ID.getText(), documentSetID)
                    .field(ESVarNames.DOC_ID.getText(), documentID)
                    .field(ESVarNames.PDF_TEXT.getText(), encodedText)
                .endObject();
    }

    /**
     *
     * @param indexName
     * @param doc_type
     */
    public void indexTo(String indexName, String doc_type) throws Exception {
        client.ping();
        setJson();
        String id = getDocumentID();
        IndexResponse indexResponse = client.getClient()
                .prepareIndex(indexName, doc_type)
                .setSource(getJson())
                .setId(id)
                .execute().actionGet();
        if (indexResponse.isCreated()) {
            System.out.printf("Document [%s\\%s\\%s] was successfully indexed\n", indexName, doc_type, id);
        }
    }

    public static void main(String[] args) {

    }
}