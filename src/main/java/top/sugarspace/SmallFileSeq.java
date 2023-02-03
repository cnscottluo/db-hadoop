package top.sugarspace;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

public class SmallFileSeq {
    public static void main(String[] args) throws Exception {
        // 生成SequenceFile文件
        write("/Users/cnscottluo/Downloads/大数据课件/bigdata_course_materials/hadoop/mapreduce+yarn/smallFile", "/seqFile");
        // 读取SequenceFile文件
        read("/seqFile");
    }

    private static void write(String inputDir, String outputFile) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");
        FileSystem fs = FileSystem.get(configuration);

        // 删除输出文件
        fs.delete(new Path(outputFile), true);

        SequenceFile.Writer.Option[] options = {
                SequenceFile.Writer.file(new Path(outputFile)),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)
        };

        SequenceFile.Writer writer = SequenceFile.createWriter(configuration, options);
        File inputDirPath = new File(inputDir);

        if (inputDirPath.isDirectory()) {
            File[] files = inputDirPath.listFiles();
            assert files != null;
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
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");
        FileSystem fs = FileSystem.get(configuration);

        SequenceFile.Reader.Option[] options = {
                SequenceFile.Reader.file(new Path(inputFile))
        };

        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, options);

        Text key = new Text();
        Text value = new Text();

        while (reader.next(key, value)) {
            System.out.println("文件名： " + key.toString());
            System.out.println("内容: " + value.toString());
        }

        reader.close();
    }
}
