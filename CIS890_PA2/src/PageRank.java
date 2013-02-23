
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;





public class PageRank {
	static int N = 0;
	static double d = 0.85;
	static int AMPLIFIER = 10000;
	static double notchanged = 0.1;
	static boolean converge = true;
	
	public static class CreateLinkGraphMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) {
			try {
			String line = value.toString();
			//collector.collect(new Text("0"), new Text(line));
			StringTokenizer tokenizer = new StringTokenizer(line);
			String title = new String("");
			String text = new String("");
			String word = new String();
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();	
			    if (word.trim().contains("<title>") && word.trim().contains("</title>")) {
			    	title = word.replaceAll("<title>", "").replaceAll("</title>", "");
			    	N++;
			    }
			    else {
			    	if (word.trim().contains("<title>") && tokenizer.hasMoreTokens()) {
						title = word.replaceAll("<title>", "")+" ";
						word = tokenizer.nextToken();
						while (!word.trim().contains("</title>")) {																		
							title = title.concat(word+" ");
							word = tokenizer.nextToken();
						}
						title = title.concat(word.replaceAll("</title>", ""));	
						N++;
						
					}
			    }
			    
			    if (word.trim().contains("<text>") && word.trim().contains("</text>")) {
			    	text = word.replaceAll("<text>", "").replaceAll("</text>", "");
			    }
			    else {
			    	if (word.trim().contains("<text>") && tokenizer.hasMoreTokens()) {
			    		text = word.replaceAll("<text>", "")+" ";
						word = tokenizer.nextToken();
						while (!word.trim().contains("</text>")) {																		
							text = text.concat(word+" ");
							word = tokenizer.nextToken();
						}
						text = text.concat(word.replaceAll("</text>", ""));	
						
					}
			    }
			}
			Pattern pstart = Pattern.compile("\\[\\[");
			Matcher mstart = pstart.matcher(text);
			Pattern pend = Pattern.compile("\\]\\]");
			Matcher mend = pend.matcher(text);
			while (mstart.find()) {
				int start = mstart.end();
				
				if  (mend.find(start)) {
					int end = mend.start();
					if (start < end) {
						String link = text.substring(start, end);
						collector.collect(new Text(title), new Text(link));
					}
				}
				
				
			}


			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}
	
	
	public static class CreateLinkGraphReducer extends MapReduceBase implements Reducer<Text, Text, Text, PairOfDoubleArrayList> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, PairOfDoubleArrayList> collector, Reporter reporter) throws IOException {
			ArrayListWritableComparable<Text> outlinks = new ArrayListWritableComparable<Text> ();
			while (values.hasNext()) {
				outlinks.add(new Text (values.next().toString()));
			}
			if (N==0) N=1;
			double PR = (double)1/(double)N;
			PairOfDoubleArrayList lists = new PairOfDoubleArrayList(PR, outlinks);
			collector.collect(key, lists);
		}

	}
	
	public static class ProcessPageRankMapper extends MapReduceBase implements Mapper<Text, PairOfDoubleArrayList, Text, DoubleORPairDoubleArrayList> {

		public void map(Text key, PairOfDoubleArrayList value, OutputCollector<Text, DoubleORPairDoubleArrayList> collector, Reporter reporter) throws IOException {
			ArrayListWritableComparable<Text> outlinks = value.getRightElement();
			int n = outlinks.size();
			double PRY = value.getLeftElement();
			for (Text outlink : outlinks) {				
				collector.collect(outlink, new DoubleORPairDoubleArrayList(PRY/(double)n));
				
			}
			
			collector.collect(key, new DoubleORPairDoubleArrayList(value));
			
		}
	}
	
	public static class ProcessPageRankReducer extends MapReduceBase implements Reducer<Text, DoubleORPairDoubleArrayList, Text, PairOfDoubleArrayList> {
		public void reduce(Text key, Iterator<DoubleORPairDoubleArrayList> values, OutputCollector<Text, PairOfDoubleArrayList> collector, Reporter reporter) throws IOException {
			double PR = 1-d;
			double lastPR = 0;;
			ArrayListWritableComparable<Text> outlinks = null;
			while (values.hasNext()) {
				DoubleORPairDoubleArrayList<Text> value = values.next();
				if (value.isDouble())
					PR += value.getDouble();
				else {
					outlinks = new ArrayListWritableComparable<Text>(value.getPairDoubleArrayList().getRightElement());
					lastPR = value.getPairDoubleArrayList().getLeftElement();
				}
					
				
			}
			if (java.lang.Math.abs(PR - lastPR) >= notchanged ) converge = false;
			collector.collect(key, new PairOfDoubleArrayList(PR, outlinks));
			
		}
	}	
	
	public static class CleanUpSortingMapper extends MapReduceBase implements Mapper<Text, PairOfDoubleArrayList, DoubleWritable, Text> {			
		public void map(Text key, PairOfDoubleArrayList value, OutputCollector<DoubleWritable, Text> collector, Reporter reporter) throws IOException {
			collector.collect(new DoubleWritable(value.getLeftElement()*AMPLIFIER), key);
		}
	}
	public static class CleanUpSortingReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> collector, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				collector.collect(values.next(),key);
			}
		}
	}
		
	
	public static void CreateLinkGraph(Configuration configuration)  {
		JobConf conf = new JobConf(configuration, PageRank.class);
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(PairOfDoubleArrayList.class);	     
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(SequenceFileOutputFormat.class);	    
	    conf.setMapperClass(CreateLinkGraphMapper.class);
	    conf.setReducerClass(CreateLinkGraphReducer.class);	       	        
	    FileInputFormat.setInputPaths(conf, new Path("RawData"));
	    SequenceFileOutputFormat.setOutputPath(conf, new Path("PR1"));	 
	    conf.setNumMapTasks(20);
	    conf.setNumReduceTasks(20);
		try {
			JobClient.runJob(conf);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void ProcessPageRank(Configuration configuration, int x)  {
		JobConf conf = new JobConf(configuration, PageRank.class);
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(DoubleORPairDoubleArrayList.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(PairOfDoubleArrayList.class);	     
	    conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(SequenceFileOutputFormat.class);	    
	    conf.setMapperClass(ProcessPageRankMapper.class);
	    conf.setReducerClass(ProcessPageRankReducer.class);	       	        
	    SequenceFileInputFormat.setInputPaths(conf, new Path("PR"+new Integer(x).toString()));
	    SequenceFileOutputFormat.setOutputPath(conf, new Path("PR"+new Integer(x+1).toString()));
	    conf.setNumMapTasks(20);
	    conf.setNumReduceTasks(20);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public static void CleanUpSorting(Configuration configuration,int x)  {
		JobConf conf = new JobConf(configuration, PageRank.class);
	    conf.setMapOutputKeyClass(DoubleWritable.class);
	    conf.setMapOutputValueClass(Text.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);	     
	    conf.setInputFormat(SequenceFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);	    
	    conf.setMapperClass(CleanUpSortingMapper.class);
	    conf.setReducerClass(CleanUpSortingReducer.class);	       	        
	    SequenceFileInputFormat.setInputPaths(conf, new Path("PR"+new Integer(x).toString()));
	    FileOutputFormat.setOutputPath(conf, new Path("CleanData"));	 
	    conf.setNumMapTasks(20);
	    conf.setNumReduceTasks(20);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i=0; i<children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) 
					return false;
			}
		}
		return dir.delete();
	} 
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration configuration = new Configuration();
		CreateLinkGraph(configuration);
		int x=1;
		converge = false;
		while (!converge && x <= 12) {
			converge = true;
			ProcessPageRank(configuration,x);
			deleteDir(new File("PR"+new Integer(x).toString()));
			x++;
		}
			
		CleanUpSorting(configuration,x);
		//deleteDir(new File("PR"+new Integer(x).toString()));
	}

}