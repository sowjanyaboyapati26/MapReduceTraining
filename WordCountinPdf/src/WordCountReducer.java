import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class WordCountReducer<KEY> extends Reducer<KEY, LongWritable,
                                                 KEY,LongWritable> {
 
  private LongWritable result = new LongWritable();
 
  public void reduce(KEY key, Iterable<LongWritable> values,
                     Context context) throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable val : values) {
      sum += val.get();
    }
    if (sum>9)
    result.set(sum);
    
    context.write(key, result);
  }
 
}

