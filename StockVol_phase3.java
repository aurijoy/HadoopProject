
public class StockVol_phase3 {
	public static class Map3 extends Mapper< Object,Text,Text,Text >  {
		private Text key3 = new Text();
		private Text value3 = new Text();
        public void map(Object key, Text values, Context context)throws IOException, InterruptedException {
			String val3 = values.toString(); 
			String keys3 = key.toString();
      		key3.set("null");		                               
            value3.set(val3);
			context.write(key3, value3);
       }
    }
        
        public static class Red3 extends Reducer<Text, Text, Text, Text> {
		    private Text key_out3 = new Text();
			private Text value_out3 = new Text();
	      	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			
		
			HashMap<String,String> hashmap  = new HashMap<String,String>();
            TreeMap<String,String> treemap = new TreeMap<String,String>();                
       
            for (Text val : values) {
                	String[] col = val.toString().split("// ");
			    	hashmap.put( col[2],col[0]);
			    	System.out.println(col[0].toString());
			    	System.out.println(col[2].toString());
			         
                }
            treemap.putAll(table); 
            
            //http://www.tutorialspoint.com/java/util/treemap_descendingkeyset.htm
            //http://stackoverflow.com/questions/12365613/retrieving-last-n-values-from-treemap
            ArrayList <string> 10_Most_Volatile= new ArrayList<String>();
            new ArrayList<String>()
            map startValues = new ArrayList<map.Entry<String, String>>(10);
            iterator = treeMap.entrySet().iterator();
            for (int i1 = 0; iterator.hasNext() && i1 < 10; i1++) {
                startValues.add(iterator.next());
                10_Most_Volatile[i]=iterator.next().getValue();
                key_out3.set(10_Most_Volatile[i]);
     			value_out3.set(it.next().getValue(););
     			context.write(key_out3, value_out3);
            }
            
            NavigableSet nset=treeMap.descendingKeySet();
            ////http://stackoverflow.com/questions/12365613/retrieving-last-n-values-from-treemap
            //Creating iterator
            ArrayList <string> 10_Least_Volatile= new ArrayList<String>();
            int counter=0;
            Iterator itr = nset.iterator();
            while(itr.hasNext() && counter < 10) {
            	10_Least_Volatile[i]=iterator.next().getValue()
                System.out.print(element + " ");
            	counter++;
            	key_out3.set(10_Least_Volatile[i]);
     			value_out3.set(it.next().getValue(););
     			context.write(key_out3, value_out3);
            }
            
   			}

}
