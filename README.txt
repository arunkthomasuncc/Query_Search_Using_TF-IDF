Creating the jar file
---------------------------------------------------------
1. Open Cloudera VM and create a java project in Eclipse
2. Add the WordCount.java,TermFrequency.java,TFIDF.java and Search.java to the src folder
3. Right click on the project and click on Build path -> Configure Build path -> Add External Jar 
   3.a go to usr/lib/hadoop and add all jars except the folders
   3.b same location add jars inside client-0.20 folder
   If you are running the project in eclipse, you may need to add jackson-core-asl.jar and jackson-mapper-asl.jar
4. After adding the jars, make sure there is no error in the files.
5. Right click on the project, click export -> java -> jar file and give name as wordcounttfidf.jar and location as
   /home/cloudera/Desktop/wordcounttfidf/wordcounttfidf.jar

Input files
----------------------------------------------------------
1. Download the canterbury zip from http://corpus.canterbury.ac.nz/resources/cantrbry.zip .
2. unzip the file and copy only  alice29.txt, asyoulik.txt, cp.html, fields.c, grammar.lsp, Icet10.txt, plrabn12.txt, xargs.1
   to /home/cloudera/Desktop/wordcounttfidf/input folder
3. open the terminal and execute the following commands to copy the files to hdfs input directory
   $ sudo su hdfs
   $ hadoop fs -mkdir /user/cloudera
   $ hadoop fs -chown cloudera /user/cloudera
   $ exit
   $ sudo su cloudera
   $ hadoop fs -mkdir /user/cloudera/wordcounttfidf /user/cloudera/wordcounttfidf/input 
   $ hadoop fs -put /home/cloudera/Desktop/wordcounttfidf/input/*  /user/cloudera/wordcounttfidf/input

Running WordCount
-------------------------------------------------------------

1. execute the following command
       $ hadoop jar <jar location> WordCount <hdfs input file location> <hdfs output file location>
   ex: $ hadoop jar wordcounttfidf.jar WordCount /user/cloudera/wordcounttfidf/input /user/cloudera/wordcounttfidf/wordcount/output
2. You can see the output 
       $ hdfs fs -cat /user/cloudera/wordcounttfidf/wordcount/output/*
3. you can copy the result to local machine (cloudera vm) from hdfs location by 
	   $ hdfs fs -get /user/cloudera/wordcounttfidf/wordcount/output/part-r-00000 <local location>

Running TermFrequency
-------------------------------------------------------------

1. execute the following command
       $ hadoop jar <jar location> TermFrequency <hdfs input file location> <hdfs output file location>
   ex: $ hadoop jar wordcounttfidf.jar TermFrequency /user/cloudera/wordcounttfidf/input /user/cloudera/wordcounttfidf/termfrequency/output
2. You can see the output 
       $ hdfs fs -cat /user/cloudera/wordcounttfidf/termfrequency/output/*
3. you can copy the result to local machine (cloudera vm) from hdfs location by 
	   $ hdfs fs -get /user/cloudera/wordcounttfidf/termfrequency/output/part-r-00000 <local location>
	   
Running TFIDF
--------------------------------------------------------------
Note : Output of TFIDF program is stored inside a tfidf folder in output file location
1. execute the following command
       $ hadoop jar <jar location> TFIDF <hdfs input file location> <hdfs output file location>
   ex: $ hadoop jar wordcounttfidf.jar TFIDF /user/cloudera/wordcounttfidf/input /user/cloudera/wordcounttfidf/tfidf/output
2. You can see the output 
       $ hdfs fs -cat /user/cloudera/wordcounttfidf/tfidf/output/tfidf/*
3. you can copy the result to local machine (cloudera vm) from hdfs location by 
	   $ hdfs fs -get /user/cloudera/wordcounttfidf/tfidf/output/tfidf/part-r-00000 <local location>
	   
Running Search
------------------------------
1. execute the following command
       $ hadoop jar <jar location> Search <TFIDF output location> <hdfs output file location> <User query in quotes>
   ex: $ hadoop jar wordcounttfidf.jar Search /user/cloudera/wordcounttfidf/tfidf/output/tfidf /user/cloudera/wordcounttfidf/serach/output "computer science"
2. You can see the output 
       $ hdfs fs -cat /user/cloudera/wordcounttfidf/search/output/*
3. you can copy the result to local machine (cloudera vm) from hdfs location by 
	   $ hdfs fs -get /user/cloudera/wordcounttfidf/search/output/part-r-00000 <local location>

To run the Search again give a different output folder or delete the existing output folder 


Note: While calculating IDF score I am considering Total number of docs as long so IDF score omit the decimal points from result.
So result may vary
  