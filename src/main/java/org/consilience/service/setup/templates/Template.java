package org.consilience.service.setup.templates;

/**
 * Created by cloudera on 7/13/15.
 */

import org.consilience.conf.IndexConfig;
import org.consilience.conf.IndexConfig.SupportedLocale;
import org.consilience.helpers.ESKeywords;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.consilience.helpers.ESVarNames;
import org.consilience.indexer.StartTCPService;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Template {
    private static final Logger logger = LoggerFactory.getLogger(Template.class);

    private String templateContent;
    private String templateName;
    private boolean forceCreation;
    private StartTCPService client;
    private IndexConfig config;

    public Template(StartTCPService client, IndexConfig config, String templateName, boolean forceCreation) {
        this.client = client;
        this.config = config;
        this.templateName = templateName;
        this.forceCreation = forceCreation;
    }

    private String getTemplateContent() {
        return templateContent;
    }

    private String getTemplateName() {
        return templateName;
    }

    private void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    private boolean isForceCreation() {
        return forceCreation;
    }

    private void setForceCreation(boolean forceCreation) {
        this.forceCreation = forceCreation;
    }

    public void setTemplateContent() throws IOException {
        XContentBuilder xContentBuilder = jsonBuilder()
            .startObject()
                .field(ESKeywords.TEMPLATE.getText(), ESVarNames.TEMPLATE_INDEX_PATTERN.getText())
                .startObject(ESKeywords.SETTINGS.getText())
                    .field(ESKeywords.INDEX_STORE_TYPE.getText(), ESKeywords.DEFAULT.getText())
                    .startObject(ESKeywords.INDEX.getText())
                        .field(ESKeywords.NUMBER_OF_SHARDS.getText(), 5)
                        .field(ESKeywords.NUMBER_OF_REPLICAS.getText(), 1)
                        .field(ESKeywords.INDEX_REFRESH_INTERVAL.getText(), "60s")
                    .endObject()
                    .startObject(ESKeywords.ANALYSIS.getText())
                        .startObject(ESKeywords.FILTER.getText())
                            .startObject(this.config.getPresOrigFilterName())
                                .field(ESKeywords.TYPE.getText(), ESKeywords.WORD_DELIMITER.getText())
                                .field("preserve_original", true)
                            .endObject()
                            .startObject(this.config.getAposReplaceFilterName())
                                .field(ESKeywords.TYPE.getText(), "pattern_replace")
                                .field("pattern", ".*\\'$")
                                .field("replacement", "")
                            .endObject()
                            .startObject(this.config.getWordDelimFilterName())
                                .field(ESKeywords.TYPE.getText(), ESKeywords.WORD_DELIMITER.getText())
                                .field(ESKeywords.STEM_ENGLISH_POSSESSIVE.getText(), true)
                            .endObject()
                            .startObject(this.config.getStemmerFilterName())
                                .field(ESKeywords.TYPE.getText(), "stemmer")
                                .field(ESKeywords.NAME.getText(), "porter")
                            .endObject()
                            .startObject(this.config.getStopWordFilterDefaultName())
                                .field(ESKeywords.TYPE.getText(), ESKeywords.STOP.getText())
                                .field(ESKeywords.NAME.getText(), "_english_")
                            .endObject()
                            .startObject(this.config.getStopWordFilterSnowballName())
                                .field(ESKeywords.TYPE.getText(), ESKeywords.STOP.getText())
                                .field(ESKeywords.STOPWORDS_PATH.getText(), "snowball.stop")
                            .endObject()
                            .startObject(this.config.getStopWordFilterSmartName())
                                .field(ESKeywords.TYPE.getText(), ESKeywords.STOP.getText())
                                .field(ESKeywords.STOPWORDS_PATH.getText(), "smart.stop")
                            .endObject()
                            .startObject(this.config.getShingleFilterName())
                                .field(ESKeywords.TYPE.getText(), ESKeywords.SHINGLE.getText())
                                .field(ESKeywords.MIN_SHINGLE_SIZE.getText(), ESVarNames.MIN_SHINGLE_VALUE.getText())
                                .field(ESKeywords.MAX_SHINGLE_SIZE.getText(), ESVarNames.MAX_SHINGLE_VALUE.getText())
                                .field(ESKeywords.OUTPUT_UNIGRAMS.getText(), true)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();

        this.templateContent = xContentBuilder.prettyPrint().string();
    }

    /**
     *
     * @param templateName
     * @return  true iff templateName exists
     */
    private boolean isTemplateExist(String templateName) {
        return !this.client.getClient().admin().indices().prepareGetTemplates(templateName).get().getIndexTemplates().isEmpty();
    }

    /**
     *
     * @param templateName
     */
    public void deleteTemplate(String templateName) {
        DeleteIndexTemplateResponse deleteIndexTemplateResponse =
                this.client.getClient().admin().indices().prepareDeleteTemplate(templateName).execute().actionGet();
        if (deleteIndexTemplateResponse.isAcknowledged())
            System.out.println("Template [" + templateName + "] deleted");
    }

    /**
     *
     * @param template template name
     * @param force set true iff you want to overwrite
     *              existing template of the same name
     *              as @param template
     * @throws Exception
     */
    private void createTemplate(String template, boolean force) throws Exception {
        if (logger.isTraceEnabled())
            logger.trace("createTemplate(" + template + ")");

        if (force && isTemplateExist(template)) {
            if (logger.isDebugEnabled())
                logger.debug("Force remove template [" + template + "]");
            DeleteIndexTemplateResponse deleteIndexTemplateResponse = client.getClient().admin()
                    .indices()
                    .prepareDeleteTemplate(template)
                    .execute().actionGet();
            if(!deleteIndexTemplateResponse.isAcknowledged())
                throw new Exception("Could not delete template [" + template + "]");
        }

        setTemplateContent();
        String source = getTemplateContent();
        if (source != null) {
            if (logger.isTraceEnabled())
                logger.trace("Template [" + template + "]=" + source);
            final PutIndexTemplateResponse response = client.getClient().admin().indices()
                    .preparePutTemplate(template)
                    .setSource(source)
                    .execute()
                    .actionGet();
            if (!response.isAcknowledged()) {
                throw new Exception("Could not define template [" + template + "].");
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Template [" + template + "] successfully created.");
                }
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("No template definition for [" + template + "]. Ignoring.");
            }
        }
        if (logger.isTraceEnabled())
            logger.trace("createTemplate(" + template + ")");
    }

    public void init() throws Exception {
        createTemplate(getTemplateName(), isForceCreation());
    }

    public static void main(String[] args) throws Exception {
        StartTCPService client = new StartTCPService();
        IndexConfig Config;
        SupportedLocale locale = SupportedLocale.ENGLISH;
        Config = new IndexConfig("asdf", locale);
        Template newTemplate = new Template(client, Config, ESVarNames.TEMPLATE_NAME.getText(), false);
        newTemplate.init();
    }
}
