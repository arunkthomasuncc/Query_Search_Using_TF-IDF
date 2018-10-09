/* 
 * Author: Arun K Thomas
 * email : akunnump@uncc.edu
 * Date  : 10/03/2018
 */

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/*
 * This program will give the output as each unique word and its number of occurrence in each file 
 * 
 */
public class WordCount extends Configured implements Tool {
	
	static String delimiter="#####";

   private static final Logger LOG = Logger .getLogger( WordCount.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new WordCount(), args);
      System .exit(res);
   }
   
   /* Job is configured here based on the input and output files, Mapper and Reducer class*/

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " wordcount ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /* Map will take produce the intermediate result and it will pass as input to reducer. Output of mapper is Text as key and IntWritable as value */
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();
         // filename is found using below method
         String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
         LOG.log(Priority.INFO, fileName);
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            word=word.toLowerCase();
            word=word+WordCount.delimiter+fileName;
            currentWord  = new Text(word);
            context.write(currentWord,one);
         }
      }
   }

   /*
    * Reducer takes the input from mapper and its in the form of Text as key (word#####filename) and Iterable of IntWritable as value. it will calculate the total number of occurrence in each file . Output will be 
    * stored in given output location in HDFS
    * */
   
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         context.write(word,  new IntWritable(sum));
      }
   }
}