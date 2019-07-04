package com.dgmall.spark.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Cedaris
 * @Date: 2019/7/4 15:47
 */
public class BeamDemo {
    public static void main(String[] args) {
        //创建PipeLine
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PipelineOptions options1 = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        //创建自定义option
        PipelineOptionsFactory.register(MyOptions.class);
        PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyOptions.class);
        //创建PCollection
        //Create the PCollection 'lines' by applying a 'Read' transform.
        PCollection<String> lines = pipeline.apply(
                "ReadMyFile", TextIO.read().from("protocol://path/to/some/inputData.txt")
        );
        // Create a Java Collection, in this case a List of Strings.
        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        // Apply Create, passing the list and the coder, to create the PCollection.
        PCollection<String> memoryCollection = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

        //TODO : Transforms
        PCollection<Integer> wordLenths = memoryCollection.apply(ParDo.of(new ComputerWordLengthFn()));
        /*
        // 将具有匿名DoFn的ParDo应用于PCollection字
        // 将结果保存为PCollection wordLengths.
        PCollection<Integer> wordLengths = words.apply(
          "ComputeWordLengths",                     // the transform name
          ParDo.of(new DoFn<String, Integer>() {    // a DoFn as an anonymous inner class instance
              @ProcessElement
              public void processElement(ProcessContext c) {
                c.output(c.element().length());
              }
            }));

            // App一个MapElements与匿名lambda功能的PCollection
            // 将结果保存为PCollection wordLengths.
            PCollection<Integer> wordLengths = words.apply(
              MapElements.into(TypeDescriptors.integers())
                         .via((String word) -> word.length()));
         */

    }
    static class ComputerWordLengthFn extends DoFn<String ,Integer>{
        @ProcessElement
        public void processElement(ProcessContext p){
            // Get the input element from ProcessContext.
            String word = p.element();
            /*
            如果您的输入“PCollection”中的元素是键/值对，则可以使用 可以通过使用访问密钥或值 ProcessContext.element().getKey()
            or ProcessContext.element().getValue()
             */
            // Use ProcessContext.output to emit the output element.
            p.output(word.length());
        }
    }
}
