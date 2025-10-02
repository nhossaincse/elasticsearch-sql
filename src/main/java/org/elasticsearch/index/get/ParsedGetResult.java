/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.get;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.get.GetResult.FIELDS;
import static org.elasticsearch.index.get.GetResult.FOUND;
import static org.elasticsearch.index.get.GetResult._ID;
import static org.elasticsearch.index.get.GetResult._INDEX;
import static org.elasticsearch.index.get.GetResult._PRIMARY_TERM;
import static org.elasticsearch.index.get.GetResult._SEQ_NO;
import static org.elasticsearch.index.get.GetResult._VERSION;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class ParsedGetResult {

    public static GetResult fromXContentEmbedded(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return fromXContentEmbedded(parser, null, null);
    }

    public static GetResult fromXContentEmbedded(XContentParser parser, String index, String id) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        long version = -1;
        long seqNo = UNASSIGNED_SEQ_NO;
        long primaryTerm = UNASSIGNED_PRIMARY_TERM;
        Boolean found = null;
        BytesReference source = null;
        Map<String, DocumentField> documentFields = new HashMap<>();
        Map<String, DocumentField> metaFields = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (_INDEX.equals(currentFieldName)) {
                    index = parser.text();
                } else if (_ID.equals(currentFieldName)) {
                    id = parser.text();
                } else if (_VERSION.equals(currentFieldName)) {
                    version = parser.longValue();
                } else if (_SEQ_NO.equals(currentFieldName)) {
                    seqNo = parser.longValue();
                } else if (_PRIMARY_TERM.equals(currentFieldName)) {
                    primaryTerm = parser.longValue();
                } else if (FOUND.equals(currentFieldName)) {
                    found = parser.booleanValue();
                } else {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, Collections.singletonList(parser.objectText())));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SourceFieldMapper.NAME.equals(currentFieldName)) {
                    try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                        // the original document gets slightly modified: whitespaces or pretty printing are not preserved,
                        // it all depends on the current builder settings
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder);
                    }
                } else if (FIELDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        DocumentField getField = DocumentField.fromXContent(parser);
                        documentFields.put(getField.getName(), getField);
                    }
                } else {
                    parser.skipChildren(); // skip potential inner objects for forward compatibility
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (IgnoredFieldMapper.NAME.equals(currentFieldName) || IgnoredSourceFieldMapper.NAME.equals(currentFieldName)) {
                    metaFields.put(currentFieldName, new DocumentField(currentFieldName, parser.list()));
                } else {
                    parser.skipChildren(); // skip potential inner arrays for forward compatibility
                }
            }
        }
        return new GetResult(index, id, seqNo, primaryTerm, version, found, source, documentFields, metaFields);
    }

    public static GetResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        return fromXContentEmbedded(parser);
    }
}
