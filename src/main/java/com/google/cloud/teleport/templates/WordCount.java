/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.protocol.types.Field;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.google.cloud.teleport.templates.DLPTextToBigQueryStreaming.LOG;
/**
 * An template that counts words in Shakespeare.
 */
public class WordCount {

  public static class UnzipFN extends DoFn< MatchResult.Metadata,Long>{
    private long filesUnzipped=0;
    @ProcessElement
    public void processElement(ProcessContext c){
      MatchResult.Metadata p = c.element();
      GcsUtil.GcsUtilFactory factory = new GcsUtil.GcsUtilFactory();
      GcsUtil u = factory.create(c.getPipelineOptions());
      byte[] buffer = new byte[100000000];
      try{
        SeekableByteChannel sek = u.open(GcsPath.fromUri(p.toString()));
        InputStream is = Channels.newInputStream(sek);
        BufferedInputStream bis = new BufferedInputStream(is);
        ZipInputStream zis = new ZipInputStream(bis);
        ZipEntry ze = zis.getNextEntry();
        while(ze!=null){
          LOG.info("Unzipping File {}",ze.getName());
          WritableByteChannel wri = u.create(GcsPath.fromUri("gs://bucket_location/" + ze.getName()), getType(ze.getName()));
          OutputStream os = Channels.newOutputStream(wri);
          int len;
          while((len=zis.read(buffer))>0){
            os.write(buffer,0,len);
          }
          os.close();
          filesUnzipped++;
          ze=zis.getNextEntry();


        }
        zis.closeEntry();
        zis.close();

      }
      catch(Exception e){
        e.printStackTrace();
      }
      c.output(filesUnzipped);
      System.out.println(filesUnzipped+"FilesUnzipped");
      LOG.info("FilesUnzipped");
    }

    private String getType(String fName){
      if(fName.endsWith(".zip")){
        return "application/x-zip-compressed";
      }
      else {
        return "text/plain";
      }
    }
  }

  public static void main(String... args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

     pipeline.apply(FileIO.match().filepattern("INPUT FILE"))
            .apply(ParDo.of(new UnzipFN()));

    pipeline.run().waitUntilFinish();
  }
}
