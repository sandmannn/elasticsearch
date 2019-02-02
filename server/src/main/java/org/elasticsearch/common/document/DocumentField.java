/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.document;

import com.carrotsearch.hppc.ObjectHashSet;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;

/**
 * A single field name and values part of {@link SearchHit} and {@link GetResult}.
 *
 * @see SearchHit
 * @see GetResult
 */
public class DocumentField implements Streamable, ToXContentFragment, Iterable<Object> {

    private static String isMetadataFieldKey = "isMetadata";
    private static String FieldValuesKey = "value";
//    {"field":{"FieldValues":["value1","value2"],"isMetadataField":true}}
    private static ObjectHashSet<String> META_FIELDS = ObjectHashSet.from(
            "_id", "_type", "_routing", "_index",
            "_size", "_timestamp", "_ttl", IgnoredFieldMapper.NAME
    );
    private String name;
    private Boolean isMetadataField;
    private List<Object> values;

    private DocumentField() {
    }

    public DocumentField(String name, List<Object> values, boolean isMetadataField) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.values = Objects.requireNonNull(values, "values must not be null");
        this.isMetadataField = isMetadataField;
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * The first value of the hit.
     */
    public <V> V getValue() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return (V)values.get(0);
    }

    /**
     * The field values.
     */
    public List<Object> getValues() {
        return values;
    }

    /**
     * @return The field is a metadata field
     */
    public boolean isMetadataField() {
//        return this.isMetadataField;
        return MapperService.isMetadataField(name);
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    public static DocumentField readDocumentField(StreamInput in) throws IOException {
        DocumentField result = new DocumentField();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        int size = in.readVInt();
        values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readGenericValue());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(values.size());
        for (Object obj : values) {
            out.writeGenericValue(obj);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // new serialization
        builder.startObject(name);
        builder.startArray(FieldValuesKey);
        for (Object value : values) {
          // this call doesn't really need to support writing any kind of object.
          // Stored fields values are converted using MappedFieldType#valueForDisplay.
          // As a result they can either be Strings, Numbers, or Booleans, that's
          // all.
          builder.value(value);          
        }
        
        builder.endArray();
        builder.field(isMetadataFieldKey, true);        
        builder.endObject();
        
        
        
        
        // old serialization
        
//        builder.startArray(name);
//        for (Object value : values) {
//            // this call doesn't really need to support writing any kind of object.
//            // Stored fields values are converted using MappedFieldType#valueForDisplay.
//            // As a result they can either be Strings, Numbers, or Booleans, that's
//            // all.
//            builder.value(value);
//        }
//        builder.endArray();
        return builder;
    }

    public static DocumentField fromXContent(XContentParser parser, boolean insideSource) throws IOException {
//         when decoding from a string:
//         we need to check if there is a key refering to existence of a metadata information.
//        if so, we are decoding it according to new rules.
//        if not, we are decoding it in old way, but calling the mapper service to determine if it is 
        // a metadata field
        
        
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String fieldName = parser.currentName();
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_ARRAY) {
            // old format
            List<Object> values = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                values.add(parseFieldsValue(parser));
            }
            return new DocumentField(fieldName, values, !insideSource);
            
        }   else {
            String message = "Failed to parse object: unexpected token [%s] found";
            throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, message, token));
        }
        
//        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser::getTokenLocation);
//        List<Object> values = new ArrayList<>();
//        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
//            values.add(parseFieldsValue(parser));
//        }
//        boolean isMetadataField = MapperService.isMetadataField(fieldName);        
//        return new DocumentField(fieldName, values, isMetadataField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DocumentField objects = (DocumentField) o;
        return Objects.equals(name, objects.name) && Objects.equals(values, objects.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values);
    }

    @Override
    public String toString() {
        return "DocumentField{" +
                "name='" + name + '\'' +
                ", values=" + values +
                '}';
    }
}
