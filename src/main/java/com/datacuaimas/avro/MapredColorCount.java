package com.datacuaimas.avro;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import example.avro.User;

public class MapredColorCount extends Configured implements Tool {

  public static class ColorCountMapper extends AvroMapper<User, Pair<CharSequence, Integer>> {
    @Override
    public void map(User user, AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
        throws IOException {
      CharSequence color = user.getFavoriteColor();
      // We need this check because the User.favorite_color field has type ["string", "null"]
      if (color == null) {
        color = "none";
      }
      collector.collect(new Pair<CharSequence, Integer>(color, 1));
      }
  }

  public static class ColorCountReducer extends AvroReducer<CharSequence, Integer, Pair<CharSequence, Integer>> {
    @Override
    public void reduce(CharSequence key, Iterable<Integer> values, AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
      throws IOException {
        int sum = 0;
        for (Integer value : values) {
          sum += value;
        }
        collector.collect(new Pair<CharSequence, Integer>(key, sum));
      }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MapredColorCount <input path> <output path>");
      return -1;
    }

    JobConf conf = new JobConf(getConf(), MapredColorCount.class);
    conf.setJobName("colorcount");

    // If Output directory already exists, delete it
    Path outputPath = new Path(args[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    AvroJob.setMapperClass(conf, ColorCountMapper.class);
    AvroJob.setReducerClass(conf, ColorCountReducer.class);

    // Note that AvroJob.setInputSchema and AvroJob.setOutputSchema set
    // relevant config options such as input/output format, map output
    // classes, and output key class.
    AvroJob.setInputSchema(conf, User.getClassSchema());
    AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.STRING), Schema.create(Type.INT)));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapredColorCount(), args);

    if (res == 0) {
      File outputDir = new File(args[1]);
      File[] outputFiles = outputDir.listFiles();
      for (File outputFile : outputFiles) {
        if (outputFile.getName().endsWith(".avro")) {
          String textName = outputFile.getName().replace(".avro", ".txt");
          // Deserializar los datos del archivo Avro. 
          List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath());
          // Escribir los registros deserializados en un archivo de texto.
          File textFile = new File(outputFile.getParent(), textName);
          FileUtils.writeLines(textFile, records);
        }
      }
      System.out.println("Job executed successfully");
    } else {
      System.out.println("Job failed");
    }

    System.exit(res);
  }
}
