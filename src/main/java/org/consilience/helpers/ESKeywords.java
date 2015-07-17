package org.consilience.helpers;

/**
 * Created by cloudera on 6/24/15.
 */

public enum ESKeywords {
    INCLUDE_IN_ALL("include_in_all"),
    ANALYSIS("analysis"),
    FILTER("filter"),
    TYPE("type"),
    NAME("name"),
    STOP("stop"),
    STOPWORDS_PATH ("stopwords_path"),
    SNOWBALL("snowball"),
    LANGUAGE("language"),
    WORD_DELIMITER("word_delimiter"),
    PROTECTED_WORDS_PATH("protected_words_path"),
    TYPE_TABLE_PATH("type_table_path"),
    SYNONYM("synonym"),
    SYNONYMS_PATH("synonyms_path"),
    ANALYZER("analyzer"),
    INDEX_ANALYZER("index_analyzer"),
    SEARCH_ANALYZER("search_analyzer"),
    CUSTOM("custom"),
    TOKENIZER("tokenizer"),
    WHITESPACE("whitespace"),
    LOWERCASE("lowercase"),
    CHAR_FILTER("char_filter"),
    HTML_STRIP("html_strip"),
    KEYWORD("keyword"),
    STANDARD("standard"),
    PROPERTIES("properties"),
    DATE("date"),
    DATE_FORMATS("date_formats"),
    FORMAT("format"),
    STORE("store"),
    YES("yes"),
    INDEX("index"),
    NOT_ANALYZED("not_analyzed"),
    ANALYZED("analyzed"),
    FLOAT("float"),
    BOOLEAN("boolean"),
    STRING("string"),
    DOUBLE("double"),
    FIELDS("fields"),
    MULTI_FIELD("multi_field"),
    INDEX_MAPPER_DYNAMIC("index.mapper.dynamic"),
    INDEX_STORE_TYPE("index.store.type"),
    DEFAULT("default"),
    _DEFAULT_("_default_"),
    DYNAMIC("dynamic"),
    STRICT("strict"),
    SOURCE("_source"),
    ENABLED("enabled"),
    INTEGER("integer"),
    CLUSTER_NAME("cluster.name"),
    PATH_DATA("path.data"),
    PATH_WORK("path.work"),
    PATH_LOG("path.log"),
    PATH_CONF("path.conf"),
    NUMBER_OF_SHARDS("number_of_shards"),
    NUMBER_OF_REPLICAS("number_of_replicas"),
    ANALYZER_SIMPLE("simple"),
    SYNONYMS_IGNORE_CASE("ignore_case"),
    SYNONYMS_EXPAND("expand"),
    NESTED("nested"),
    ATTACHMENT("attachment"),
    ATTACHMENT_DATE("date"),
    ATTACHMENT_TITLE("title"),
    ATTACHMENT_NAME("name"),
    ATTACHMENT_AUTHOR("author"),
    ATTACHMENT_KEYWORDS("keywords"),
    ATTACHMENT_CONTENT_TYPE("content_type"),
    ATTACHMENT_CONTENT_LENGTH("content_length"),
    ATTACHMENT_LANG("language"),
    INDEX_REFRESH_INTERVAL("refresh_interval"),
    SHINGLE("shingle"),
    MIN_SHINGLE_SIZE("min_shingle_size"),
    MAX_SHINGLE_SIZE("max_shingle_size"),
    OUTPUT_UNIGRAMS("output_unigrams"),
    TERM_VECTOR("term_vector"),
    SETTINGS("settings"),
    MAPPINGS("mappings"),
    TEMPLATE("template"),
    WITH_POSITION_OFFSETS("with_positions_offsets"),
    FIELDDATA("fielddata"),
    FREQUENCY("frequency"),
    MIN_FREQUENCY("min"),
    MAX_FREQUENCY("max")
    ;

    private String text;

    public String getText() {
        return text;
    }

    ESKeywords(String word) {
        this.text = word;
    }
}