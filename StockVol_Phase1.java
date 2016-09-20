
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import sun.text.resources.cldr.ee.FormatData_ee;

import com.google.common.collect.Iterators;

	public class StockVol_phase1 {

			public static class Phase1_Mapper extends Mapper<LongWritable, Text, Text, Text> {
				private Text key = new Text();
                private Text value = new Text();
				public void map(LongWritable keys, Text values, Context context)throws IOException, InterruptedException {

					String line = values.toString();                       
					FileSplit fileSplit = (FileSplit)context.getInputSplit();
					String cname = fileSplit.getPath().getName();       
					String cvsSplitBy=",";
					String[] col = line.split(cvsSplitBy);                    
					
					String dateSplitter= "/";
					String[] cdate = col[0].split(dateSplitter);                  
					key.set(cname + " " + cdate[0] + " " + cdate[2]);  //add cdate[1]
				    value.set(cdate[1] + " " + col[6]);              
	                context.write(key, value);                       
	                }
	 
			}

		
          public static class Phase1_Reducer extends Reducer<Text, Text, Text, Text> {
			private Text key_out = new Text();       
			private Text value_out = new Text();
  			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				String val = values.toString();
                String keys = key.toString();
                 
                HashMap<String, String> hashMap = new HashMap<String, String>();
                TreeMap<String, String> treeMap = new TreeMap<String, String>();
                treeMap.putAll(hashMap);
                
                for (Text v1 : val) {
	        	hashmap.put(keys, val);
		  } 
   
                treeMap.putAll(hashMap);				/*http://www.thecodingforums.com/threads/newbie-converting-a-hashmap-to-a-treemap.132543/   */
                
                String pricekey1 = treeMap.firstKey();    //http://www.tutorialspoint.com/java/util/treemap_firstkey.htm
                String pricevalue1 = treemap.get(pricekey1);

                String pricekey2 = treeMap.lastKey();	//http://www.tutorialspoint.com/java/util/treemap_lastkey.htm
                String pricevalue2 = treemap.get(pricekey2);
                
                String rr = (pricevalue2-pricevalue1)/(pricevalue1));
				key_out.set(keys);
				value_out.set(rr);
				context.write(key_out,value_out);
			}			
          }
          
          
//-------------------------------------------------------------------------------------------------------------------------------
	  