package com.dgmall.spark.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * @Author: Cedaris
 * @Date: 2019/7/4 15:51
 */
public interface MyOptions extends PipelineOptions {
    @Description("My custom command line argument")
    @Default.String("DEFAULT")
    String getMyCustomOption();
    void setMyCustomOption(String myCustomOption);
}
