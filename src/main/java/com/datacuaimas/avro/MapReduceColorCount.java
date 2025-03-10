package com.datacuaimas.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import classes.avro.User;

public class MapReduceColorCount extends Configured implements Tool {
  public static class ColorCountMapper extends
      Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {

    /**
     * Maps an input record to a set of intermediate key-value pairs.
     * @param key The input key
     * @param value The input value
     * @param context The context to which the output key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(AvroKey<User> key, NullWritable value, Context context)
        throws IOException, InterruptedException {

      CharSequence color = key.datum().getFavoriteColor();
      if (color == null) {
        color = "none";
      }
      context.write(new Text(color.toString()), new IntWritable(1));
    }
  }

  public static class ColorCountReducer extends
      Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {

    /**
     * Reduce the values for a key to a single output value.
     * @param key The key for which the values are being passed.
     * @param values The values for the given key.
     * @param context The context to which the output should be written.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
     context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Integer>(sum));
    }
  }

  /**
   * Runs the MapReduce job.
   * @param args Arguments to the job
   * @return 0 if the job completes successfully, 1 otherwise
   * @throws Exception If an error occurs
   */
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MapReduceColorCount <input path> <output path>");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(MapReduceColorCount.class);
    job.setJobName("Color Count");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapperClass(ColorCountMapper.class);
    AvroJob.setInputKeySchema(job, User.getClassSchema());
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    job.setReducerClass(ColorCountReducer.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Main method for running the job.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new MapReduceColorCount(), args);
    System.exit(res);
  }
}
