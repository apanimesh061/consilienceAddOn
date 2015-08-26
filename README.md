Elasticsearch for Consilience!
===================

**Elasticsearch** is being used for indexing a large number of documents that will be analyzed using Apache Spark and R routines.

----------

Settings vs. Mappings
-------------

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
