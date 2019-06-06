/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.templates;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * An example that counts words in Shakespeare and includes Beam best practices.
 *

 *
 * <p>For a detailed walkthrough of this example, see <a
 * href="https://beam.apache.org/get-started/wordcount-example/">
 * https://beam.apache.org/get-started/wordcount-example/ </a>
 *
 * <p>Basic concepts, also in the MinimalWordCount example: Reading text files; counting a
 * PCollection; writing to text files
 *
 * <p>New Concepts:
 *
 * <pre>
 *   1. Executing a Pipeline both locally and using the selected runner
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline either locally or using by selecting another runner.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * <p>To execute this pipeline, specify a local output file (if using the {@code DirectRunner}) or
 * output prefix on a supported distributed file system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with {@code --inputFile}.
 */
public class WordCount {

  /**
   * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
   * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
   * a ParDo in the pipeline.
   */
//  static class ExtractWordsFn extends DoFn<String, String> {
//    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
//    private final Distribution lineLenDist =
//        Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");
//
//   @DoFn.ProcessElement
//    public void processElement(@Element String element, OutputReceiver<String> receiver) {
//      lineLenDist.update(element.length());
//      if (element.trim().isEmpty()) {
//        emptyLines.inc();
//      }
//
//      // Split the line into words.
//      String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);
//
//      // Output each word encountered into the output PCollection.
//      for (String word : words) {
//        if (!word.isEmpty()) {
//          receiver.output(word);
//        }
//      }
//    }
//  }
//
//  /** A SimpleFunction that converts a Word and Count into a printable string. */
//  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
//    @Override
//    public String apply(KV<String, Long> input) {
//      return input.getKey() + ": " + input.getValue();
//    }
//  }
//
//  /**
//   * A PTransform that converts a PCollection containing lines of text into a PCollection of
//   * formatted word counts.
//   *
//   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
//   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
//   * modular testing, and an improved monitoring experience.
//   */
//  public static class CountWords
//      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
//    @Override
//    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
//
//      // Convert lines of text into individual words.
//      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
//
//      // Count the number of times each word occurs.
//      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());
//
//      return wordCounts;
//    }
//  }
//
//  /**
//   * Options supported by {@link WordCount}.
//   *
//   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
//   * be processed by the command-line parser, and specify default values for them. You can then
//   * access the options values in your pipeline code.
//   *
//   * <p>Inherits standard configuration options.
//   */
//  public interface WordCountOptions extends PipelineOptions {
//
//    /**
//     * By default, this example reads from a public dataset containing the text of King Lear. Set
//     * this option to choose a different input file or glob.
//     */
//    @Description("Path of the file to read from")
//    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
//    String getInputFile();
//
//    void setInputFile(String value);
//
//    /** Set this required option to specify where to write the output. */
//    @Description("Path of the file to write to")
//    @Required
//    String getOutput();
//
//    void setOutput(String value);
//  }
//
//  static void runWordCount(WordCountOptions options) {
//    Pipeline p = Pipeline.create(options);
//
//    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
//    // static FormatAsTextFn() to the ParDo transform.
//    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
//        .apply(new CountWords())
//        .apply(MapElements.via(new FormatAsTextFn()))
//        .apply("WriteCounts", TextIO.write().to(options.getOutput()));
//
//    p.run().waitUntilFinish();
//  }

  public static void main(String[] args) {
    try {
      StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder()
              .setCredentials(ServiceAccountCredentials
                      .fromStream(new FileInputStream("computeEngineSA.json")));
    } catch (IOException e) {
      e.printStackTrace();
    }

    Pipeline p = Pipeline.create(
            PipelineOptionsFactory.fromArgs().withValidation().create());
    GcsUtil.GcsUtilFactory factory = new GcsUtil.GcsUtilFactory();
    GcsUtil util = factory.create(p.getOptions());
    try{
      List<GcsPath> gcsPaths = util.expand(GcsPath.fromUri("gs://uspto_cybris/cybris.ms-dev.zip"));
      List<String> paths = new ArrayList<String>();

      for(GcsPath gcsp: gcsPaths){
        paths.add(gcsp.toString());
      }
      p.apply(Create.of(paths))
              .apply(ParDo.of(new UnzipFN()));
      p.run();
    }
    catch(Exception e){
      LoggerFactory.getLogger("TTTTTTTTTTTTTT").error(e.getMessage());
    }
  }

  public static class UnzipFN extends DoFn<String,Long>{
    private static final long serialVersionUID = 2015166770614756341L;
    private long filesUnzipped=0;

    @ProcessElement
    public void processElement(ProcessContext c){
      String p = c.element();
      GcsUtil.GcsUtilFactory factory = new GcsUtil.GcsUtilFactory();
      GcsUtil u = factory.create(c.getPipelineOptions());
      byte[] buffer = new byte[100000000];
      try{
        SeekableByteChannel sek = u.open(GcsPath.fromUri(p));
        InputStream is;
        is = Channels.newInputStream(sek);
        BufferedInputStream bis = new BufferedInputStream(is);
        ZipInputStream zis = new ZipInputStream(bis);
        ZipEntry ze = zis.getNextEntry();
        while(ze!=null){
          LoggerFactory.getLogger("TTTTTTTTTTTTTTTT").info("Unzipping File {}",ze.getName());
          WritableByteChannel wri = u.create(GcsPath.fromUri("gs://uspto_cybris/" + ze.getName()), getType(ze.getName()));
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
}