Elasticsearch for Consilience!
===================

**Elasticsearch** is being used for indexing a large number of documents that will be analyzed using Apache Spark and R routines.

----------

Settings vs. Mappings
-------------

####Settings 

Settings are the list of pre-processings that are applied on a given index. In Consilience we are dealing with three types of pre-processings:

 - `token_filter`: These filters are applied on tokens. Our project is using `lowercase`, `word-delimiter`, `stop`, `shingle` and `apostrophe`.

     - `lowercase`: This converts all the tokens to lowercase before any processing
     - `word-delimiter`: This is for handling apostrophe problems where apostrophe is deleted if it occurs at the end of a token. Also there is a `preserve_original` type which also saves the original terms after processing is done.
     - `stop`: This filter removes the stop-words during the pre-processing.
     - `shingle`: This helps in creating `unigrams`, `bigrams` and `trigrams` of the terms of a specific document.

 - `tokenizer`: This takes care about how a string should be split into tokens. We use `standard` as our tokenizer type which splits according to the white-space as the delimiter and also takes care of `punctuation`.

 - `char_filter`: This applies on characters. Useful when we want to manipulate characters before indexing. In our application we want to take care of quotes and convert to a normalized character so that during tokenizing it does not give wrong tokens.

    |                  | TO                        | FROM              |
 ----------------- | ---------------------------- | ------------------
| Single backticks | `'Isn't this fun?'`            | 'Isn't this fun?' |
| Quotes           | `"Isn't this fun?"`            | "Isn't this fun?" |
    You can see that under TO the quotes have been normalized. These characters are easily handled by `standard` tokenizer.

####Mappings
Mappings define the schema of the index i.e. how a field is going to be processed and how it is going to be identified. Following is the project index's `mappings`:

    "mappings": {
         "document_set": {
            "dynamic": "strict",
            "properties": {
               "doc_id": {
                  "type": "string",
                  "index": "not_analyzed",
                  "include_in_all": false
               },
               "docset_id": {
                  "type": "string",
                  "index": "not_analyzed",
                  "include_in_all": false
               },
               "text": {
                  "type": "string",
                  "store": true,
                  "term_vector": "with_positions_offsets",
                  "analyzer": "analyzer_5589b14f3004fb6be70e4724",
                  "fielddata": {
                     "filter.frequency.max": "0.99",
                     "filter.frequency.min": "0.01"
                  }
               }
            }
         }
      }

Analyzers in a mappings follow a strict sequence of filters that we defined in the settings. The sequence which we follow is:

    "analyzer": {
                  "analyzer_5589b14f3004fb6be70e4724": {
                     "filter": [ 
                        ## This sequence has to be maintained
                        "lowercase",
                        "preserve_original_en_EN",
                        "porter_stemmer_en_EN",
                        "asciifolding",
                        "apos_replace_en_EN"
                     ],
                     "char_filter": [
                        "quotes_en_EN"
                     ],
                     "type": "custom",
                     "tokenizer": "standard"
                  }
               }
> **Note:**
> 
> - `filters` do not include `stop` token filters as the stop words have already been removed.
> - If a filter has been initialized, it does not mean it has to  be included in the analyzer's definition.
> - The stopwords were removed using the `Analyze API` using `DocumentWrapper.filterStopWords()`.

Recommended Design Patterns
-------------
The recommended design patters are mentioned on slide 30 of this [presentation](http://www.slideshare.net/apanimesh061/elasticsearch-and-spark-50696773/30).
Following blogs also give a fair idea about how to take care of performance:

 - http://radar.oreilly.com/2015/04/10-elasticsearch-metrics-to-watch.html
 - https://www.loggly.com/blog/nine-tips-configuring-elasticsearch-for-high-performance/

####Running ES from command line

    sudo elasticsearch 
	    -Des.cluster.name=<some name> 
	    -Des.path.conf=<some_path>/config/ 
	    -Des.path.data=<some_path>/data/ 
	    -Des.path.logs=<some_path>/logs/
	    -Xmx16g -Xms16g

This command specifies the cluster name, the required paths for the data to be stored. Last 2 parameters set 16 GB to be the maximum and minimum values for the JVM Heap memory for Elasticsearch.

> **Note:**
> 
> - JVM Heap memory should be 50% of the system's RAM but the upper limit is 32 GB.
