package top.sugarspace;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class SmallFileMap {

    public static void main(String[] args) throws IOException {
        // 生成SequenceFile文件
        write("/Users/cnscottluo/Downloads/大数据课件/bigdata_course_materials/hadoop/mapreduce+yarn/smallFile", "/mapFile");
        // 读取SequenceFile文件
        read("/mapFile");
    }

    private static void write(String inputDir, String outputFile) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer.Option[] opts = {
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(Text.class)
        };

        fs.delete(new Path(outputFile), true);

        MapFile.Writer writer = new MapFile.Writer(conf, new Path(outputFile), opts);

        File inputDirPath = new File(inputDir);
        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
            assert files != null;

            Arrays.sort(files, Comparator.comparing(File::getName));

            for (File file : files) {
                String content = FileUtils.readFileToString(file, "UTF-8");
                Text key = new Text(file.getName());
                Text value = new Text(content);
                writer.append(key, value);
            }
        }

        writer.close();
    }

    private static void read(String inputFile) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");
        FileSystem fs = FileSystem.get(conf);

        MapFile.Reader reader = new MapFile.Reader(new Path(inputFile), conf);

        Text key = new Text();
        Text value = new Text();

        while (reader.next(key, value)) {
            System.out.println("文件名： " + key.toString());
            System.out.println("内容: " + value.toString());
        }

        reader.close();
    }
}
