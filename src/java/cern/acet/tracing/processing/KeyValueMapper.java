/**
 * Logalike - A stream based message processor Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All
 * Rights Reserved. This software is distributed under the terms of the GNU General Public Licence version 3 (GPL
 * Version 3), copied verbatim in the file “COPYLEFT”. In applying this licence, CERN does not waive the privileges and
 * immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any
 * jurisdiction. Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing.processing;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Message;
import cern.acet.tracing.util.Tuple;

/**
 * A {@link MessageMapper} that can extract key-value pairs from messages. Unless overwritten, the default is to parse
 * key-value pairs in the form of 'key=value' with either a comma (,), pipe (|) or semi-colon (;) as separator.
 *
 * @author jepeders
 * @param <T> The type of {@link Message}s to parse.
 */
public class KeyValueMapper<T extends Message<T>> implements MessageMapper<T> {

    private static final int KEY_ID = 0;
    private static final int VALUE_ID = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueMapper.class);

    private final String field;
    private final Pattern keyValueDelimiter;
    private final Pattern keyValuePairDelimiter;
    private final BiFunction<T, Tuple<String, String>, T> keyValueParser;

    /**
     * Constructs a KeyValueMapper that will try to extract key-value pairs from the given field of incoming messages.
     *
     * @param builder The builder whose values to use when constructing the {@link KeyValueMapper}.
     */
    private KeyValueMapper(Builder<T> builder) {
        keyValueDelimiter = builder.keyValueDelimiter;
        keyValuePairDelimiter = builder.keyValuePairDelimiter;
        keyValueParser = builder.keyValueParser;
        field = builder.field;
    }

    @Override
    public T apply(T message) {
        //@formatter:off
        return message.getOptionalAs(field, String.class)
                .map(fieldValue -> parseKeyValuePairs(fieldValue, message))
                .orElse(message);
        //@formatter:on
    }

    /**
     * @return A {@link Builder} that can help build a {@link KeyValueMapper}.
     * @param <T> The type of {@link Message}s to use in the {@link KeyValueMapper}.s
     */
    public static <T extends Message<T>> Builder<T> builder() {
        return new Builder<T>();
    }

    /**
     * Parses a number of key-value pairs and adds them to the given message.
     * 
     * @param keyValuePairsString The string containing key-value pairs to parse.
     * @param message The message to merge the resulting key-value pair into.
     * @return A message with the key-value pair.
     */
    public T parseKeyValuePairs(String keyValuePairsString, T message) {
        final String[] keyValuePairs = keyValuePairDelimiter.split(keyValuePairsString);
        T parsedMessage = message;
        for (int pairNumber = 0; pairNumber < keyValuePairs.length; pairNumber++) {
            final String keyValue = keyValuePairs[pairNumber];
            if (!keyValue.isEmpty()) {
                Optional<Tuple<String, String>> keyValueOption = splitToKeyAndValue(keyValue);
                if (keyValueOption.isPresent()) {
                    parsedMessage = keyValueParser.apply(parsedMessage, keyValueOption.get());
                }
            }
        }
        return parsedMessage;
    }

    private Optional<Tuple<String, String>> splitToKeyAndValue(String keyValue) {
        final String[] keyAndValue = keyValueDelimiter.split(keyValue);
        if (keyAndValue.length < 2) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Failed to split string '{}' into a key-value pair using the regex delimiter '{}'",
                        keyValue, keyValueDelimiter);
            }
            return Optional.empty();
        } else {
            if (LOGGER.isDebugEnabled() && keyAndValue.length > 3) {
                LOGGER.debug("Expected two fields for key and value pair, but found {}. Input: {}, Delimiter: {}",
                        keyAndValue.length, keyValue, keyValueDelimiter);
            }

            String key = keyAndValue[KEY_ID];
            String value = keyAndValue[VALUE_ID];
            return Optional.of(Tuple.of(key, value));
        }
    }

    /**
     * A builder that can help to build a {@link KeyValueMapper}.
     * 
     * @author jepeders
     * @param <T> The type of {@link Message} to use when building a {@link KeyValueMapper}.
     */
    public static class Builder<T extends Message<T>> {

        private String field;
        private BiFunction<T, Tuple<String, String>, T> keyValueParser = (message, tuple) -> message.put(
                tuple.getFirst(), tuple.getSecond());
        private Pattern keyValueDelimiter = Pattern.compile("(?<!\\\\)=");
        private Pattern keyValuePairDelimiter = Pattern.compile("(?<!\\\\)(,|;)");

        /**
         * Builds a {@link KeyValueMapper} from the current parameters.
         * 
         * @return An instance of a {@link KeyValueMapper}.
         * @throws NullPointerException if the field has not been set.
         */
        public KeyValueMapper<T> build() {
            Objects.requireNonNull(field, "The message field must be set");
            return new KeyValueMapper<T>(this);
        }

        /**
         * Defines the field to parse in {@link Message}s sent to the {@link KeyValueMapper}. This is a mandatory field
         * that must be set before the {@link KeyValueMapper} can be built.
         * 
         * @param field The name of the field to match in incoming messages, e. g. "body".
         * @return The same builder with the field set to the given value.
         */
        public Builder<T> setFieldToParse(String field) {
            this.field = field;
            return this;
        }

        /**
         * Defines the delimiter between key and value in a key-value pair (NOT between different key-value pairs), so
         * the parser can extract key and value from a single string. The default behaviour is to split on equal-signs
         * (=).
         * 
         * @param regex The regular expression that, when matched, splits the input string.
         * @return The same builder with the key-value delimiter set to the given value.
         */
        public Builder<T> setKeyValueDelimiter(String regex) {
            this.keyValueDelimiter = Pattern.compile(regex);
            return this;
        }

        /**
         * Defines the delimiter between different key-value pairs (NOT between key and value), so the parser can
         * extract different key-value pairs from a single string. The default behaviour is to split on commas (,),
         * pipes (|) or semi-colons (;).
         * 
         * @param regex The regular expression that, when matched, splits the input string.
         * @return The same builder with the key-value delimiter set to the given value.
         */
        public Builder<T> setKeyValuePairDelimiter(String regex) {
            this.keyValuePairDelimiter = Pattern.compile(regex);
            return this;
        }

        /**
         * Chooses the parser that will handle key-value
         * 
         * @param field The name of the field to match in incoming messages, e. g. "body".
         * @return The same builder with the field set to the given value.
         */
        public Builder<T> setKeyValueParser(BiFunction<T, Tuple<String, String>, T> keyValueParser) {
            this.keyValueParser = keyValueParser;
            return this;
        }

    }

}
