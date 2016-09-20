import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Main {

	public static void main(String[] args) {
		long start = new Date().getTime();
		Configuration config = new Configuration();
		
		Job job1 = Job.getInstance();
		job.setJarByClass(StockVol_phase1.class);
		job job2 = Job.getInstance();
		job.setJarByClass(StockVol_phase2.class);
		Job job3 = Job.getInstance();
	    job.setJarByClass(StockVol_phase3.class);
	   		
		job1.setJarByClass(StockVol_phase1.class);
		job1.setMapperClass(StockVol_phase1.Phase1_Mapper.class);
		job1.setReducerClass(StockVol_phase1.Phase1_Reducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		int NOfReducer1 = Integer.valueOf(args[2]);	
		job1.setNumReduceTasks(NOfReducer1);
		
		job2.setJarByClass(StockVol_phase2.class);
		job2.setMapperClass(StockVol_phase2.Phase2_Mapper.class);
		job2.setReducerClass(StockVol_phase2.Phase2_Reducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		int NOfReducer2 = Integer.valueOf(args[2]);	
		job2.setNumReduceTasks(NOfReducer2);
		
		
		job3.setJarByClass(StockVol_phase3.class.class);
		job3.setMapperClass(StockVol_phase3.Phase3_Reducer.class);
		job3.setReducerClass(StockVol_phase3.Phase3_Reducer.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
        int NOfReducer3 = 1;	
		job3.setNumReduceTasks(NOfReducer3);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job1, new Path("Inter_"+args[1]));
		
		FileInputFormat.addInputPath(job2, new Path("Inter_"+args[1]));
		FileOutputFormat.setOutputPath(job2, new Path("Output_"+args[1]));
		
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path("Inter_"+args[1]));
		

		job1.waitForCompletion(true);
		job2.waitForCompletion(true);
		job3.waitForCompletion(true);
		boolean status3 = job3.waitForCompletion(true);
		if (status3 == true) {
			long end = new Date().getTime();

			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}		

	}
		
		
		
		
		
		
		
		
		

	}

}
