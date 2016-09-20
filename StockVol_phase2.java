import java.util.LinkedList;
import java.util.List;


public class StockVol_phase2 {

    public static class Phase2_Mapper extends Mapper< Text,Text,Text,Text >  {
		private Text key2 = new Text();
		private Text value2 = new Text();
        public void map(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
			String keys = key.toString(); 
			String values = value.toString();
			String[] col = keys.split(" "); 
         key2.set(cols[0]);               
         value2.set(values);               
         }
  
	
  
    public static class Phase2_Reducer extends Reducer<Text, Text, Text, Text> {
	    private Text key_out2 = new Text();
		private Text value_out2 = new Text();
		private List<String> rrlist = new LinkedList<String>();
     	public void reduce(Text keys, Iterable<Text> record, Context context)throws IOException, InterruptedException {
        	
				String keys2 = keys.toString();
	            
	            double N=0;
				for (Text text : record.toString()) {
					rrlist.add(record);
					N+=N;
				}   
				double sumofxi = 0;        				
				for (int i = 0; i < record.length; i++) {
			    sumofxi = sumofxi + Double.parseDouble(rrlist[i]);
				
				}
              double xbar = totalsum/sum;
              for (String record : rrlist) {
					
					double total = Math.pow((Double.parseDouble(alist)-xbar),2)/(sum-1);
				}  				   		            
              double vol= double sqrt(total);   
              key_out2.set(keys2);
              value_out2.set(vol);
              context.write(key_out2,value_out2);
			}
    }
}

}
