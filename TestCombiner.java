package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;

public class TestCombiner {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,AvgEntity> {
        private Text okey =new Text();
        private AvgEntity avgEntity=new AvgEntity();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String [] strs=line.split(" ");
            okey.set(strs[0]);
            avgEntity.setCount(1);
            avgEntity.setSum(Integer.parseInt(strs[1]));
            context.write(okey,avgEntity);
        }
    }

    public static class ForCombiner extends Reducer<Text,AvgEntity,Text,AvgEntity> {
        ForCombiner(){
            System.out.println("===============================================");
        }
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum=0;
            for(AvgEntity i:values){
                count++;
                sum+=i.getSum();
            }
            AvgEntity avgEntity=new AvgEntity();
            avgEntity.setCount(count);
            avgEntity.setSum(sum);
            context.write(key,avgEntity);
        }
    }
    public static class ForReducer extends  Reducer<Text,AvgEntity,Text,IntWritable>{
        ForReducer(){
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum=0;
            for(AvgEntity avgEntity:values){
                count+=avgEntity.getCount();
                sum+=avgEntity.getSum();
            }
            context.write(key,new IntWritable(sum/count));
            System.out.println("0000000000000000000000000000000000000000000000");
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(TestCombiner.class,"D:\\AVG","");
    }
}
