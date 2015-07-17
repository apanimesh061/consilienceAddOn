package org.consilience.conf;

/**
 * Created by cloudera on 7/13/15.
 */

import org.apache.commons.lang.LocaleUtils;

import java.util.Locale;

public class IndexConfig {
    private String indexName;
    private SupportedLocale supportedLocale;

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setSupportedLocale(SupportedLocale supportedLocale) {
        this.supportedLocale = supportedLocale;
    }

    public String getIndexName() {
        return indexName;
    }

    public SupportedLocale getSupportedLocale() {
        return supportedLocale;
    }

    public IndexConfig(String indexName, SupportedLocale supportedLocale) {
        this.indexName = indexName;
        this.supportedLocale = supportedLocale;
    }

    public static enum SupportedLocale {
        ENGLISH("en_EN", "English");

        private String text;
        private String lang;

        SupportedLocale(String text, String lang) {
            this.text = text;
            this.lang = lang;
        }

        public String getText() {
            return text;
        }

        public String getLang() {
            return lang;
        }
    }

    public Locale getLocale() {
        return LocaleUtils.toLocale(supportedLocale.getText());
    }

    public String getStemmerFilterName() {
        return "porter_stemmer_" + getSupportedLocale().getText();
    }

    public String getStopWordFilterDefaultName() {
        return "default_stop_name_" + getSupportedLocale().getText();
    }

    public String getStopWordFilterSnowballName() {
        return "snowball_stop_words_" + getSupportedLocale().getText();
    }

    public String getStopWordFilterSmartName() {
        return "smart_stop_words_" + getSupportedLocale().getText();
    }

    public String getShingleFilterName() {
        return "shingle_filter_" + getSupportedLocale().getText();
    }
}
