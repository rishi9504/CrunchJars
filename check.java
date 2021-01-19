package com.example;
import org.apache.crunch.*;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Join;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;

public class Example3Crunch {
	public static void main(String[] args) throws Exception {
		// Store Arguments in Variables
				/*String inputPath = args[0];
				String outputPath = args[1];*/
				// Create and Configure Data Pipeline
				Pipeline pipeline = MemPipeline.getInstance();
				 
				// Read Data from File into Crunch PCollection
				PCollection<String> data1 = pipeline.readTextFile("/home/rishi9504/cruncht/crunch-demo/input/SampleCSV1109.csv");
				PCollection<String> data2 = pipeline.readTextFile("/home/rishi9504/cruncht/crunch-demo/input/SampleCSV5000.csv");
				System.out.println("Number of lines ingested from data1"	+ data1.length().getValue());
				System.out.println("Number of lines ingested from data2"	+ data2.length().getValue());
				//System.out.println(data1);
				PCollection updatedData1 = data1
						.parallelDo(DoFn_ChangeData(), Writables.strings());
				PCollection updatedData2 = data2
						.parallelDo(DoFn_ChangeData(), Writables.strings());
				
				PTable<Long, String> updatedData3 =  updatedData1
						.parallelDo(DoFn_ChangeData1(), Avros.tableOf(Avros.longs(), Avros.strings()));
				PTable<Long, String> updatedData4 =  updatedData2
						.parallelDo(DoFn_ChangeData1(), Avros.tableOf(Avros.longs(), Avros.strings()));
				//System.out.println(updatedData3);
				//System.out.println(updatedData4.values());
				//PTable<Long, String> maxes = updatedData3.groupByKey().combineValues((CombineFn<Long, String>) Aggregators.MAX_LONGS(10));
				//System.out.println(maxes);
				//JoinStrategy<Long, String, String> strategy=Join.leftJoin(updatedData3, updatedData4)
				PTable<Long, Pair<String, String>> joined = Join.leftJoin(updatedData3, updatedData4);
				joined.write(To.textFile("/home/rishi9504/cruncht/crunch-demo/opdemo/SampleCSV1109.csv"),WriteMode.OVERWRITE);
				//end the pipeline and exit the program
				PipelineResult result = pipeline.done();
				System.exit(result.succeeded() ? 0 : 1);
	}
	static DoFn<String, String> DoFn_ChangeData(){
	    return new DoFn<String, String>() {
	    @Override
	    public void process(String input, Emitter emitter) {
	       String[] inputParts = input.split("\n");
	       String personName = inputParts[0];
	       emitter.emit(personName);	 
	}
	};
	}
	
	static DoFn<String,Pair<Long,String>> DoFn_ChangeData1(){
	    return new DoFn<String, Pair<Long,String>>() {
	    @Override
	    public void process(String input, Emitter emitter) {
	    	//process the input in a way that it gets splitted into
	    	//first colmn data: key
	    	//second colmn data:value
	    	
	       String[] inputParts = input.split(",");
	       
	       String data1key = inputParts[0];
	       String data1value = inputParts[1]+","+inputParts[2];
	       //emitter.emit(personName);	
	       emitter.emit(Pair.of(Long.parseLong(data1key), data1value));
	}
	};
	}
	
	
	
}
