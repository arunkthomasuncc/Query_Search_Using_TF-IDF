/*
 * Author: Arun K Thomas
 * email : akunnump@uncc.edu
 * 
 */
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/*
 * This program is used to find the TFIDF score for each unique word for each file.
 * TF-IDF(t, d) = WF(t,d) * IDF(t) 
 * 
 */

public class TFIDF extends Configured implements Tool {
	
	static String delimiter="#####";
	// static variable not working when I run in hdfs
	//static long totalNumberOfDocs=0;

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   /*
    * I am creating a temp folder in output directory to store the TermFrequency job result. This will be the input for Mapper in the TFIDF job.
    * First I am executing TermFrequency  map Reducer  and once it is completed I am running the TFIDF Map Reducer by passing the ouput from TermFrequecny map reducer.
    * 
    * Input for TermFrequency MapReduce is Input files location and Temp folder location in output directory
    * Input for TFIDF MapReduce is input file location, output file location of TermFrequency MapReduce and output directory.
    * Output of TFIDF MapReduce will be stored in tfidf folder of output directory.
    */
   public static void main( String[] args) throws  Exception {
	   
	  String inputFileDir= args[0];
	  String outputDirForTF=args[1]+"/temp";
	  String[] inputForTFargs= {inputFileDir,outputDirForTF};
	  int tfres= ToolRunner.run(new TermFrequency(), inputForTFargs);  
	  String[] inputForTFIDFargs = {args[0],args[1]+"/tfidf",outputDirForTF}; 
	  if(tfres ==0)
	  {
        int tfidfres = ToolRunner .run( new TFIDF(), inputForTFIDFargs);
        System .exit(tfidfres);
	  }
      System .exit(tfres);
   }
   /*
    *  Job is configured here based on the input and output files, Mapper and Reducer class
    *  I am calcuatling the total number of files in the hdfs input file directiry and storing it as a parameter in Configuration. This is used from the Reducer.
    *  
    */
   public int run( String[] args) throws  Exception {
      //3 arguments passed
	  // first argument the locations where all files reside, second one output directory, third is the location of output of TermFrequency program.
	  
	  
	  
	  FileSystem fs = FileSystem.get(getConf());
	  Path pt = new Path(args[0]);
	  ContentSummary cs = fs.getContentSummary(pt);
	  long fileCount = cs.getFileCount();
	  
	  Configuration config= new Configuration();
	  config.set("totalnumberofdocs", fileCount+"");
	  Job job  = Job .getInstance(config, " tfidf ");
      job.setJarByClass( this .getClass());
	  
	//static variable not working when I ran in hdfs
	//  totalNumberOfDocs=fileCount;
    //  FileInputFormat.addInputPaths(job,  args[0]);
      FileInputFormat.addInputPaths(job,  args[2]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /* 
    * Map will read the output from TermFrequency MapReducer and for each word as key it produce the filename=tfscore as value.
    */
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         String[] keyValues=line.split(TFIDF.delimiter);
         String key=keyValues[0];
         String[] fileNameTF=keyValues[1].split("\t");
         String fileName= fileNameTF[0];
         String tfScore=fileNameTF[1];
         
         String result=fileName+"="+tfScore;
         
          context.write(new Text(key), new Text(result));
         }
      
   }
   /*Reduce will calculate the tfidf score of each word for each file. totat number of files is read from the configutation and it is used for calculating the idf score.
    * 
    * 
    */
   
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text > tfscores,  Context context)
         throws IOException,  InterruptedException {
    	  Configuration conf = context.getConfiguration();
    	  String totalNumberOfDocsString = conf.get("totalnumberofdocs");
    	  int totalNumberOfDocs= Integer.parseInt(totalNumberOfDocsString);
         int fileCount=0;
         HashMap<String,Double> map= new HashMap<String,Double>();
         for ( Text tfscore  : tfscores) {
            String[] filetfscore= tfscore.toString().split("=");
            String fileName=filetfscore[0];
            String score=filetfscore[1];
            String key=word.toString()+TFIDF.delimiter+fileName;
            map.put(key, Double.parseDouble(score));   
            fileCount++; 
            
         }
         
         //calculating the idf
         
         double idfScore=  Math.log10(1+ (totalNumberOfDocs/fileCount));
         
         for(String key:map.keySet())
         {
        	 double tfidfScore= map.get(key)*idfScore;
        	 context.write(new Text(key),  new DoubleWritable(tfidfScore));
        	 
         }

      }
   }
}