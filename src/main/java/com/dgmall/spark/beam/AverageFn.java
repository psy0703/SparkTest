package com.dgmall.spark.beam;

import org.apache.beam.sdk.transforms.Combine;

/**
 * 如何定义一个计算的“CombineFn” 平均值：
 * @Author: Cedaris
 * @Date: 2019/7/4 16:54
 */
public class AverageFn extends Combine.CombineFn<Integer, AverageFn.Accum,
        Double> {
    public static class Accum {
        int sum = 0;
        int count = 0;
    }

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accumulator, Integer input) {
        accumulator.sum += input;
        accumulator.count++;
        return accumulator;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accumulators) {
        Accum merged = createAccumulator();
        for (Accum accum : accumulators) {
            merged.sum += accum.sum;
            merged.count += accum.count;
        }
        return merged;
    }

    @Override
    public Double extractOutput(Accum accumulator) {
        return ((double) accumulator.sum) / accumulator.count;
    }
}
