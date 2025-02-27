package com.datacuaimas.avro;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import classes.avro.User;

public class MapredColorCount extends Configured implements Tool {

  public static class ColorCountMapper extends AvroMapper<User, Pair<CharSequence, Integer>> {
    /**
     * Maps an input record to a set of intermediate key-value pairs.
     * @param user The input record
     * @param collector The collector to which the output key-value pairs are written
     * @param reporter Facility to report progress
     * @throws IOException
     */
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
  
    /**
     * Reduce the values for a key to a single output value.
     * @param key The key for which the values are being passed.
     * @param values The values for the given key.
     * @param collector The collector to which the output should be written.
     * @param reporter Facility to report progress.
     * @throws IOException
     */
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

  /**
   * The run() method is called (indirectly) from main(), and contains all the job
   * configuration and Hadoop job submission.
   * @param args The command line arguments
   * @return 0 if the Hadoop job completes successfully, 1 if not
   */
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

  /**
   * The main method specifies the Hadoop job configuration and starts the job.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapredColorCount(), args);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (res == 0) {
      Path outputDir = new Path(args[1]);
      FileStatus[] outputFiles = fs.listStatus(outputDir);
      for (FileStatus outputFile : outputFiles) {
        if (outputFile.getPath().getName().endsWith("avro")) {
          // Deserializar los datos del archivo Avro.
          // Remove the file:// prefix from the
          String route = outputFile.getPath().toString().replace("file://", "").replace(".avro", ".txt");
          List<String> records = DeserializationData.getRecords(route);
          // Escribir los registros deserializados en un archivo de texto.
          System.out.println("Aqui no funciona");
          Path outputPath = new Path(route);
          if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
          }
          FSDataOutputStream outputStream = fs.create(outputPath);
          try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))) {
            for (String line : records) {
              writer.write(line.toString());
              writer.newLine();
            }
          }
        }
      }
      System.out.println("Job executed successfully");
    } else {
      System.out.println("Job failed");
    }

    System.exit(res);
  }
}
