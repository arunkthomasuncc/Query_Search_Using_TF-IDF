/*
 * Author: Arun K Thomas
 * email : akunnump@uncc.edu
 * 
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
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
 * For a search query it will find the sum of tfidf score for each word in the search query for each file.
 * 
 */
public class Search extends Configured implements Tool {
	
	static String delimiter="#####";
	//static String userQuery="";

   private static final Logger LOG = Logger .getLogger( Search.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   /*
    * Search query is added as a parameter in the configuration
    * and it is used by Mapper.
    */
   public int run( String[] args) throws  Exception {
      
      
     
      if(args.length<=2)
      {
    	  
    	  LOG.error("Please enter search Query");
    	  
      }
      Configuration config= new Configuration();
	  config.set("userquery", args[2]);
	  Job job  = Job .getInstance(config, " search ");
      job.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /*
    * Input is the output file from TDFIDF MapReduce and it will produce the output key as File name and tfidf score as value for each word in the query
    */
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
    	  ArrayList<String> userQueryWords= new ArrayList<String>();
    	  Configuration conf = context.getConfiguration();
    	  String userQuery = conf.get("userquery");
    	  String[] userQueryValues= userQuery.split(" "); 			  
    	  for(String s: userQueryValues)
    	  {
    		  userQueryWords.add(s.toLowerCase());
    	  }
    	  
    	  String values[]= lineText.toString().split(Search.delimiter);
    	  if(userQueryWords.contains(values[0]))
    	  {
    		  String results[]= values[1].split("\t");
    		  double value= Double.parseDouble(results[1]);
    		  context.write(new Text(results[0]), new DoubleWritable(value));
    		  
    	  }
    	  
    	  
      }
   }

   /*
    * it will produce the output as file name as key and sum of tfidf score for each word in the search query as value.
    */
   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable> counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0.0;
         for ( DoubleWritable count  : counts) {
        	
            sum  += (count.get());
         }
           	 
         context.write(word,  new DoubleWritable(sum));
      }
   }
}