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


import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.parquet.format.BlockCipher;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

class AesCtrEncryptor implements BlockCipher.Encryptor{
  
  static final int CTR_NONCE_LENGTH = 16;
  static final int chunkLen = 4 * 1024;

  private final SecretKey key;
  private final SecureRandom random;

  AesCtrEncryptor(byte[] keyBytes) throws IOException {
    if (null == keyBytes) {
      throw new IOException("Null key bytes");
    }
    key = new SecretKeySpec(keyBytes, "AES");
    random = new SecureRandom();
  }

  @Override
  public byte[] encrypt(byte[] plaintext)  throws IOException {
    return encrypt(plaintext, 0, plaintext.length);
  }

  @Override
  public byte[] encrypt(byte[] plaintext, int pOffset, int pLen)  throws IOException {
    byte[] nonce = new byte[CTR_NONCE_LENGTH];
    random.nextBytes(nonce);
    IvParameterSpec spec = new IvParameterSpec(nonce);
    byte[] ciphertext;
    try {
      // Cipher is not thread safe (using 'synchronized encrypt' kills performance). Create new.
      Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, key, spec);

      int clen = pLen + CTR_NONCE_LENGTH;
      ciphertext = new byte[clen];
      int left = pLen;
      int input_offset = pOffset;
      int output_offset = CTR_NONCE_LENGTH;
      // Breaking encryption into multiple updates, to trigger h/w acceleration in Java 9, 10
      while (left > chunkLen) {
        int written = cipher.update(plaintext, input_offset, chunkLen, ciphertext, output_offset);
        input_offset += chunkLen;
        output_offset += written;
        left -= chunkLen;
      }
      cipher.doFinal(plaintext, input_offset, left, ciphertext, output_offset);
      // Add the nonce
      System.arraycopy(nonce, 0, ciphertext, 0, CTR_NONCE_LENGTH);
    }
    catch (GeneralSecurityException e) {
      throw new IOException("Failed to encrypt", e);
    }
    return ciphertext;
  }
}

