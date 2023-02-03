package top.sugarspace;

import com.sun.jersey.core.spi.scanning.FilesScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsOp {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://bigdata01:9000");
        FileSystem fs = FileSystem.get(configuration);

        // get(fs, "/README.txt", "/Users/cnscottluo/README.txt");
        delete(fs, "/README.txt");
    }


    private static void put(FileSystem fs, String inputFile, String outputFile) throws IOException {
        try (FSDataOutputStream outputStream = fs.create(new Path(outputFile))) {
            try (FileInputStream inputStream = new FileInputStream(inputFile)) {
                inputStream.transferTo(outputStream);
            }
        }
    }

    private static void get(FileSystem fs, String inputFile, String outputFile) throws IOException {
        try (FSDataInputStream inputStream = fs.open(new Path(inputFile))) {
            try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
                inputStream.transferTo(outputStream);
            }
        }
    }

    private static void delete(FileSystem fs, String fileName) throws IOException {
        if (fs.delete(new Path(fileName), true)) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }
}