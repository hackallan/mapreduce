package Homework20170805;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendPair {

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		try {

			// System.setProperty("HADOOP_USER_NAME", "hadoop");
			Job job = Job.getInstance(conf);
			job.setMapperClass(FPMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			Path inputPath = new Path("D:\\wordcount\\input");
			Path outputPath = new Path("D:\\wordcount\\output");
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}

			// 设置wordcount程序的输入路径
			FileInputFormat.setInputPaths(job, inputPath);
			// 设置wordcount程序的输出路径
			FileOutputFormat.setOutputPath(job, outputPath);

			job.setReducerClass(FPReducer.class);
			boolean r = job.waitForCompletion(true);
			System.exit(r ? 0 : 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	static class MapperList {
		public static Map<String, String> mapperList = new TreeMap<>();
	}

	static class FPMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] rowData = value.toString().split(":");
			String[] data = rowData[rowData.length - 1].split(",");
			Text key1 = null;
			for (String d : data) {
				key1 = new Text(rowData[0] + "-" + d);
				value = new Text(d);
				MapperList.mapperList.put(key1.toString(), value.toString());
				context.write(key1, value);
			}

		}
	}

	static class FPReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> list = new ArrayList<>();
			System.out.println(values);

			String[] currentarr = key.toString().split("-");
			List<Integer> countList = new ArrayList<>();
			for (String s : currentarr) {
				list.add(s);
			}

			MapperList.mapperList.forEach((k, v) -> {
				if (k.equals(list.get(0) + "-" + list.get(1)) || k.equals(list.get(1) + "-" + list.get(0))) {
					countList.add(1);
				}
			});

			// 如果大于1表示着两个是两两相互的关系 如：A-B 中有A，B
			Map<String, String> list1 = new TreeMap<>();
			Map<String, String> list2 = new TreeMap<>();
			List<String> valuesL = new ArrayList<>();
			if (countList.size() > 1) {
				MapperList.mapperList.forEach((k, v) -> {
					if (!k.equals(list.get(0) + "-" + list.get(1)) && !k.equals(list.get(1) + "-" + list.get(0))) {
						if (k.indexOf(list.get(0)) == 0) {
							list1.put(k, v);
						}

						if (k.indexOf(list.get(1)) == 0) {
							list2.put(k, v);
						}
					}

				});

				list1.forEach((k, v) -> {

					list2.forEach((k2, v2) -> {
						if (v.equals(v2)) {
							valuesL.add(v);
						}
					});

				});
				String result = "";
				for (String s : valuesL) {
					result += s + ",";
				}

				if (result != "") {
					context.write(key, new Text(result.substring(0, result.length() - 1)));
				}

			}

		}

	}

}
