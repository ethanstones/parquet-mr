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

package org.apache.parquet.crypto;

import java.io.IOException;
import java.util.Arrays;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.ColumnCryptoMetaData;

public class ColumnMetadata {
  
  private boolean encrypt;
  private boolean isEncryptedWithFooterKey;
  private String[] columnPath;
  private byte[] keyBytes;
  private byte[] keyMetaData;
  private ColumnCryptoMetaData ccmd;
  
  /**
   * Convenience constructor for regular (not nested) columns.
   * @param encrypt
   * @param name
   */
  public ColumnMetadata(boolean encrypt, String name) {
    this(encrypt, new String[] {name});
  }
  
  public ColumnMetadata(boolean encrypt, String[] path) {
    this.encrypt = encrypt;
    this.columnPath = path;
    this.isEncryptedWithFooterKey = encrypt;
  }
  
  public void setEncryptionKey(byte[] keyBytes, byte[] keyMetaData) throws IOException {
    // TODO if this object is read, throw an exception
    if (!encrypt) throw new IOException("Setting key on unencrypted column: " + Arrays.toString(columnPath));
    if (null == keyBytes) throw new IOException("Null key for " + Arrays.toString(columnPath));
    this.keyBytes = keyBytes;
    this.keyMetaData = keyMetaData;
    //TODO compare to footer key?
    this.isEncryptedWithFooterKey = false;
  }
  
  public void setEncryptionKey(byte[] keyBytes, int keyIdMetaData) throws IOException {
    byte[] metaData = BytesUtils.intToBytes(keyIdMetaData);
    setEncryptionKey(keyBytes, metaData);
  }

  String[] getPath() {
    return columnPath;
  }

  boolean isEncrypted() {
    return encrypt;
  }
  
  ColumnCryptoMetaData getColumnCryptoMetaData() {
    if (null != ccmd) return ccmd;
    ccmd = new ColumnCryptoMetaData(Arrays.asList(columnPath), encrypt, isEncryptedWithFooterKey);
    if (null != keyMetaData) {
      ccmd.setColumn_key_metadata(keyMetaData);
    }
    return ccmd;
  }

  byte[] getKeyBytes() {
    return keyBytes;
  }
}
