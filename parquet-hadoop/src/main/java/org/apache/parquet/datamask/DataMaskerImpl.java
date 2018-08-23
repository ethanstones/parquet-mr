/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.datamask;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.crypto.ParquetFileDecryptor;

import static java.lang.String.format;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.ParquetFileDecryptor.ColumnDecryptors;

public class DataMaskerImpl implements DataMasker {

  public String maskStringByHash(String input) {
    return DigestUtils.sha256Hex(input);
  }

  public long maskLongByHash(long input) {
   return (long) Long.toHexString(input).hashCode();
  }

  public MessageType removeMaskedColumns(ParquetFileDecryptor fileDecryptor, MessageType schema) throws IOException {
    Set<String> maskedColumns = new HashSet<>();
    for (Type field : schema.getFields()) {
      if (field.getName().contains("_mask")) {
        String fieldName = field.getName();
        maskedColumns.add(fieldName.substring(0, fieldName.lastIndexOf("_mask")));
      }
    }

    Set<String> encColsWithoutKey = getEncColsWithoutKey(fileDecryptor, schema);

    List<Type> newFields = new ArrayList<>();
    for (Type field : schema.getFields()) {
      String fieldName = field.getName();

      /* Three cases to add the field:
       * 1) this is masking column and masked column doesn't have key
       * 2) not masked column at all
       * 3) masked column but with key
       */

      if (field.getName().contains("_mask")) {
        // no key, masked column
        if (encColsWithoutKey.contains(fieldName.substring(0, fieldName.lastIndexOf("_mask")))) {
          newFields.add(field);
        }
      } else {
        if (!maskedColumns.contains(fieldName) || !encColsWithoutKey.contains(fieldName)) {
          newFields.add(field);
        }
      }
    }

    return new MessageType(schema.getName(), newFields);
  }

  public MessageType addMaskingColumns(MessageType schema) {
    List<Type> newFields = new ArrayList<>();
    newFields.addAll(schema.getFields());
    for (Type field : schema.getFields()) {
      org.apache.parquet.schema.ExtType extField = (org.apache.parquet.schema.ExtType) field;
      Map<String, Object> metadata = extField.getMetadata();
      if (metadata != null && metadata.containsKey("mask")) {
        org.apache.parquet.schema.ExtType maskField = new org.apache.parquet.schema.ExtType(extField, extField.getName() + "_mask");
        Map<String, Object> maskMetadata = new HashMap<>();
        maskMetadata.put("masking", true);
        newFields.add(maskField);
      }
    }
    return new MessageType(schema.getName(), newFields);
  }

    public MessageType addMaskingColumnsV1 (MessageType schema){
      List<Type> newFields = new ArrayList<>();
      for (Type field : schema.getFields()) {
        newFields.add(field);
        Map<String, Object> metadata = ((org.apache.parquet.schema.ExtType) field).getMetadata();
        if (metadata != null && metadata.containsKey("mask")) {
          org.apache.parquet.schema.ExtType maskField = new org.apache.parquet.schema.ExtType(field, field.getName() + "_mask");
          Map<String, Object> maskMetadata = new HashMap<>();
          maskMetadata.put("mask", metadata.get("mask"));
          maskField.setMetadata(maskMetadata);
          newFields.add(maskField);
        }
      }

      return new MessageType(schema.getName(), newFields);
    }

    private Set<String> getEncColsWithoutKey(ParquetFileDecryptor fileDecryptor, MessageType schema) throws IOException {
        Set<String> encColsWithoutKey = new HashSet<>();
        if (fileDecryptor == null) {
          return encColsWithoutKey;
        }
        for (ColumnDescriptor columnDescriptor : schema.getColumns()) {
          String[] path = columnDescriptor.getPath();
          ColumnDecryptors columnDecryptors = fileDecryptor.getColumnDecryptors(path);
          if (columnDecryptors.getStatus().equals(ColumnDecryptors.Status.KEY_UNAVAILABLE)) {
            //encColsWithoutKey.add(Arrays.toString(columnDescriptor.getPath()));
            String[] paths = columnDescriptor.getPath();
            if (paths.length > 0 && !paths[path.length - 1].contains("_mask")) {
              encColsWithoutKey.add(paths[path.length - 1]);
            }
          }
        }
        return encColsWithoutKey;
    }
}
