//ONUR ÖZDEMİR
//2036473



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class Hw3 {

  public static class LongestMapper extends Mapper<Object, Text, Text, IntWritable >{

    private Text samekey = new Text("samekey");
    private IntWritable w_length = new IntWritable(0);
    private int int_w_Length = 0;
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        String tmpString = itr.nextToken();
        int_w_Length = tmpString.length();
        word.set(tmpString);
        w_length.set(int_w_Length);
        context.write(samekey, w_length);
      }
    }
  }

  public static class LongestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable longest = new IntWritable();
    private Text longestText = new Text("longest");
    int cur_longest = 0;

    public void reduce(Text keys, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      for (IntWritable value : values) {
        if(value.get() > cur_longest){
          cur_longest = value.get();
        }
      }
      longest.set(cur_longest);
      context.write(longestText, longest);
    }
  }


  public static class LetterMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("", " ")," ");

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class LetterReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key,result);
    }
  }


  public static class LetterPartitioner extends Partitioner < Text, IntWritable >{

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks)
    {
      if(key.toString().charAt(0) == 'a' || key.toString().charAt(0) == 'e' || key.toString().charAt(0) == 'i' || key.toString().charAt(0) == 'o' || key.toString().charAt(0) == 'u'){
        return 0;
      }
      else{
        return 1 ;
      }
      
    }
  }

  public static class CountMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class CountReducer extends Combiner<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values)
      {
        sum += val.get();
      }
      result.set(sum);

      context.write(key,result);
    }
  }

   public static class CountPartitioner extends Partitioner < Text, IntWritable >{
   	
    private int length = 0;

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks)
    {
      length = key.toString().length();

      if(length < 4){
        return 0;
      }
      else if(length<7){
        return 1;
      }
      else{
        return 2;
      }
    }
   }



  public static void main(String[] args) throws Exception {

  	if(args[0].equals("longest")){

  		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "longest");
	    job.setJarByClass(Hw3.class);

	    job.setMapperClass(LongestMapper.class);
	    job.setCombinerClass(LongestReducer.class);
	    job.setReducerClass(LongestReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

  	}

  	else if(args[0].equals("letter")){

  		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "letter");
	    job.setJarByClass(Hw3.class);

	    job.setMapperClass(LetterMapper.class);
	    job.setPartitionerClass(LetterPartitioner.class);
	    job.setReducerClass(LetterReducer.class);

	    job.setNumReduceTasks(2);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

  	}



  	else{

  		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "count");
	    job.setJarByClass(Hw3.class);

	    job.setMapperClass(CountMapper.class);
	    job.setPartitionerClass(CountPartitioner.class);
	    job.setReducerClass(CountReducer.class);

	    job.setNumReduceTasks(3);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
    
    
  }
}
