/*
 * Author: Arun K Thomas
 * email : akunnump@uncc.edu
 * 
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/*
 * This class is used to find the term frequency of each unique word in each document.
 * WF(t,d) = 1 + log10(TF(t,d)) if TF(t,d) > 0, and 0 otherwise 
 */
public class TermFrequency extends Configured implements Tool {
	
	static String delimiter="#####";

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   /*
    *  Job is configured here based on the input and output files, Mapper and Reducer class
    */
    
   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " termfrequency ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
    
   /*
    * Map will read the input files and produce the intermediate result as Text as key and IntWritable as value
    */
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();
       //  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
         String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
         LOG.log(Priority.INFO, fileName);
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            word=word.toLowerCase();
            word=word+TermFrequency.delimiter+fileName;
            currentWord  = new Text(word);
            context.write(currentWord,one);
         }
      }
   }
   
   /*
    * Output from Map will be the input for Reducer and Term frequency is calculated based on the total number of occurrence of a word in the file
    * WF(t,d) = 1 + log10(TF(t,d)) if TF(t,d) > 0, and 0 otherwise 
    */

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         double result=0.0;
         if(sum>0)
        	 result=1+Math.log10(sum);

        	 
         context.write(word,  new DoubleWritable(result));
      }
   }
}