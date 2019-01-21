import java.io.IOException; 
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FiveWordUniqueSequence {

public static class WordCountMapper 
extends Mapper < LongWritable, Text, Text, IntWritable >
{

private final static IntWritable one = new IntWritable( 1); 
private Text word = new Text();

@Override 
public void map( LongWritable key, Text value, Context context
) throws IOException, 
InterruptedException 
{ 
	System.out.print("*********************************************Inside Map*********************************************");
	System.out.print("value: "+value);
	//StringTokenizer itr = new 
	//StringTokenizer( value.toString()); 
	//System.out.println("itr*****************: "+itr);
	//System.out.println("value*****************: "+value);
		
	String strLineWords[] = value.toString().split("\\s+");
	String words ="";
	for(int i=4;i<strLineWords.length;i++)
	{
		if(strLineWords[i-4].equalsIgnoreCase("null")==false&&strLineWords[i-4].equalsIgnoreCase(null)==false&&strLineWords[i-4].equalsIgnoreCase(" ")==false&&strLineWords[i-4].equalsIgnoreCase("")==false&&strLineWords[i-4].equalsIgnoreCase("\"")==false
				&&strLineWords[i-3].equalsIgnoreCase("null")==false&&strLineWords[i-3].equalsIgnoreCase(null)==false&&strLineWords[i-3].equalsIgnoreCase(" ")==false&&strLineWords[i-3].equalsIgnoreCase("")==false&&strLineWords[i-3].equalsIgnoreCase("\"")==false
				&&strLineWords[i-2].equalsIgnoreCase("null")==false&&strLineWords[i-2].equalsIgnoreCase(null)==false&&strLineWords[i-2].equalsIgnoreCase(" ")==false&&strLineWords[i-2].equalsIgnoreCase("")==false&&strLineWords[i-2].equalsIgnoreCase("\"")==false
				&&strLineWords[i-1].equalsIgnoreCase("null")==false&&strLineWords[i-1].equalsIgnoreCase(null)==false&&strLineWords[i-1].equalsIgnoreCase(" ")==false&&strLineWords[i-1].equalsIgnoreCase("")==false&&strLineWords[i-1].equalsIgnoreCase("\"")==false
				&&strLineWords[i].equalsIgnoreCase("null")==false&&strLineWords[i].equalsIgnoreCase(null)==false&&strLineWords[i].equalsIgnoreCase(" ")==false&&strLineWords[i].equalsIgnoreCase("")==false&&strLineWords[i].equalsIgnoreCase("\"")==false)
			words = strLineWords[i-4]+" "+strLineWords[i-3]+" "+strLineWords[i-2]+" "+strLineWords[i-1]+" "+strLineWords[i];
		System.out.println("words: "+words);
		String s[] = words.split(" ");
		int length = s.length; 
		if(length==5)
		{
		word.set(words);
		context.write( word, one); 
		}
	}
}
} 
public static class WordCountReducer 
extends 
Reducer < Text, IntWritable, Text, IntWritable > { 
private IntWritable result = new IntWritable(); 
@Override 
public void reduce( Text key, Iterable < IntWritable > values, Context context 
) throws IOException, 
InterruptedException { 
int sum = 0; 
for (IntWritable val : values) { 
sum += val.get(); 
} 
result.set( sum); 
context.write( key, result); 
} 
}

public static void main( String[] args) throws Exception { 
Configuration conf = new Configuration();
Job job = Job.getInstance( conf, "word count");

job.setJarByClass(FiveWordUniqueSequence.class);

FileInputFormat.addInputPath( job, new Path("input")); 
FileOutputFormat.setOutputPath( job, new Path("output")); 
job.setMapperClass( WordCountMapper.class); 
job.setCombinerClass( WordCountReducer.class); 
job.setReducerClass( WordCountReducer.class);

job.setOutputKeyClass( Text.class); 
job.setOutputValueClass( IntWritable.class);

System.exit( job.waitForCompletion( true) ? 0 : 1); 
} 
}