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

package org.apache.parquet.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.csv.AvroCSVReader;
import org.apache.parquet.cli.csv.CSVProperties;
import org.apache.parquet.cli.csv.AvroCSV;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetEncryptionFactory;
import org.apache.parquet.crypto.ParquetFileEncryptor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.cli.util.Codecs;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.apache.avro.generic.GenericData.Record;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

@Parameters(commandDescription="Create a file from CSV data")
public class ConvertCSVCommand extends BaseCommand {

  public ConvertCSVCommand(Logger console) {
    super(console);
  }

  @Parameter(description="<csv path>")
  List<String> targets;

  @Parameter(
      names={"-o", "--output"},
      description="Output file path",
      required=true)
  String outputPath = null;

  @Parameter(
      names={"-2", "--format-version-2", "--writer-version-2"},
      description="Use Parquet format version 2",
      hidden = true)
  boolean v2 = false;

  @Parameter(names="--delimiter", description="Delimiter character")
  String delimiter = ",";

  @Parameter(names="--escape", description="Escape character")
  String escape = "\\";

  @Parameter(names="--quote", description="Quote character")
  String quote = "\"";

  @Parameter(names="--no-header", description="Don't use first line as CSV header")
  boolean noHeader = false;

  @Parameter(names="--skip-lines", description="Lines to skip before CSV start")
  int linesToSkip = 0;

  @Parameter(names="--charset", description="Character set name", hidden = true)
  String charsetName = Charset.defaultCharset().displayName();

  @Parameter(names="--header",
      description="Line to use as a header. Must match the CSV settings.")
  String header;

  @Parameter(names="--require",
      description="Do not allow null values for the given field")
  List<String> requiredFields;

  @Parameter(names = {"-s", "--schema"},
      description = "The file containing the Avro schema.")
  String avroSchemaFile;

  @Parameter(names = {"--compression-codec"},
      description = "A compression codec name.")
  String compressionCodecName = "GZIP";

  @Parameter(names="--row-group-size", description="Target row group size")
  int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

  @Parameter(names="--page-size", description="Target page size")
  int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

  @Parameter(names="--dictionary-size", description="Max dictionary page size")
  int dictionaryPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

  @Parameter(
      names={"--overwrite"},
      description="Remove any data already in the target view or dataset")
  boolean overwrite = false;
  
  @Parameter(names={"-e", "--encrypted-file"},
      description="Convert to an encrypted Parquet file")
  boolean encrypt = false;
  
  @Parameter(names={"--key"},
      description="Encryption key (base64 string)")
  String encodedKey;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 1,
        "CSV path is required.");

    if (header != null) {
      // if a header is given on the command line, don't assume one is in the file
      noHeader = true;
    }

    CSVProperties props = new CSVProperties.Builder()
        .delimiter(delimiter)
        .escape(escape)
        .quote(quote)
        .header(header)
        .hasHeader(!noHeader)
        .linesToSkip(linesToSkip)
        .charset(charsetName)
        .build();

    String source = targets.get(0);

    Schema csvSchema;
    if (avroSchemaFile != null) {
      csvSchema = Schemas.fromAvsc(open(avroSchemaFile));
    } else {
      Set<String> required = ImmutableSet.of();
      if (requiredFields != null) {
        required = ImmutableSet.copyOf(requiredFields);
      }

      String filename = new File(source).getName();
      String recordName;
      if (filename.contains(".")) {
        recordName = filename.substring(0, filename.indexOf("."));
      } else {
        recordName = filename;
      }

      csvSchema = AvroCSV.inferNullableSchema(
          recordName, open(source), props, required);
    }
    
    ParquetFileEncryptor fileEncryptor = null;
    if (encrypt) {
      byte[] keyBytes;
      if (null == encodedKey) {
        keyBytes = new byte[16];
        for (byte i=0; i < 16; i++) {keyBytes[i] = i;}
        encodedKey = Base64.getEncoder().encodeToString(keyBytes);
        console.info("Encrypting with a sample key: " +encodedKey);
      }
      else {
        keyBytes = Base64.getDecoder().decode(encodedKey);
      }
      
      // #1
      FileEncryptionProperties eSetup = new FileEncryptionProperties(ParquetCipher.AES_GCM_V1, keyBytes, 12);
      //EncryptionSetup eSetup = new EncryptionSetup(Cipher.AES_GCM_V1, null, null);
      //EncryptionSetup eSetup = new EncryptionSetup(Cipher.AES_GCM_CTR_V1, keyBytes, 12);
      
      // #2
      //ColumnCryptodata encCol = new ColumnCryptodata(true, "pageURL");
      ColumnEncryptionProperties encCol = new ColumnEncryptionProperties(true, "pageRank");
      
      byte[] colKeyBytes = new byte[16]; 
      for (byte i=0; i < 16; i++) {colKeyBytes[i] = (byte) (i%3);}
      String columnKey = Base64.getEncoder().encodeToString(colKeyBytes);
      console.info("Column key: " +columnKey);
      
      // #3
      encCol.setEncryptionKey(colKeyBytes, 15);
      
      ArrayList<ColumnEncryptionProperties> columnMD = new ArrayList<ColumnEncryptionProperties>();
      columnMD.add(encCol);
      
      // #4
      eSetup.setColumnProperties(columnMD, false);
      
      byte[] aad = outputPath.getBytes(StandardCharsets.UTF_8);
      console.info("AAD: "+outputPath+". Len: "+aad.length);
      
      // #5
      eSetup.setAAD(aad, null);
      
      // #6
      //fileEncryptor = ParquetEncryptionFactory.createFileEncryptor(keyBytes);
      fileEncryptor = ParquetEncryptionFactory.createFileEncryptor(eSetup);     
    }
    
    Configuration conf = getConf();
//    conf.set(ParquetWriter.ENCRYPTION_KEY_PARAMETER_NAME, encodedKey);

    long count = 0;
    try (AvroCSVReader<Record> reader = new AvroCSVReader<>(
        open(source), props, csvSchema, Record.class, true)) {
        CompressionCodecName codec = Codecs.parquetCodec(compressionCodecName);
      try (ParquetWriter<Record> writer = AvroParquetWriter
          .<Record>builder(qualifiedPath(outputPath))
          .withWriterVersion(v2 ? PARQUET_2_0 : PARQUET_1_0)
          .withWriteMode(overwrite ?
              ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE)
          .withEncryptor(fileEncryptor)
          .withCompressionCodec(codec)
          .withDictionaryEncoding(true)
          .withDictionaryPageSize(dictionaryPageSize)
          .withPageSize(pageSize)
          .withRowGroupSize(rowGroupSize)
          .withDataModel(GenericData.get())
          .withConf(getConf())
          .withSchema(csvSchema)
          .build()) {
        for (Record record : reader) {
          writer.write(record);
        }
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed on record " + count, e);
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Create a Parquet file from a CSV file",
        "sample.csv sample.parquet --schema schema.avsc",
        "# Create a Parquet file in HDFS from local CSV",
        "path/to/sample.csv hdfs:/user/me/sample.parquet --schema schema.avsc",
        "# Create an Avro file from CSV data in S3",
        "s3:/data/path/sample.csv sample.avro --format avro --schema s3:/schemas/schema.avsc"
    );
  }
}