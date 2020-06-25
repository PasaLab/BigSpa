import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;



import edu.zuo.setree.client.IntraMain;

public class Test {
    ArrayList<File> fileList;
    File root;

    public Test(String pathName) {
        root = new File(pathName);
        fileList = new ArrayList<>();
    }

    public void searchFiles() {
        File[] files = root.listFiles();
        int length = files.length;
        for (int i = 0; i < length; i++) {
            if (files[i].isDirectory()) {
                root = files[i];
                searchFiles();
            } else {
                fileList.add(files[i]);
            }
        }
    }

    public void countFiles() {
        long totalSize = 0;
        System.out.println("文件数:" + fileList.size());
        for (int i = 0; i < fileList.size(); i++) {
            totalSize += fileList.get(i).length();
        }
        System.out.println("文件总大小:" + totalSize);
    }

    public static void main(String[] args) {
    	test();
//        String pathName = "D:\\Eclipse\\hdfs-class";
//        Test counter = new Test(pathName);
//        counter.searchFiles();
//        StringBuilder builder = new StringBuilder();
//        for (File f: counter.fileList) {
//        	String fileName = f.getPath();
//        	fileName = fileName.replace("D:\\Eclipse\\hdfs-class\\", "");
//        	fileName = fileName.replace(".class", "");
//        	fileName = fileName.replace("\\", ".");
//        	builder.append(fileName);
//        	builder.append("\n");
//        }
//        counter.writeOut(builder, "hdfsClass.txt");
    }
    
    protected void writeOut(StringBuilder info, String fileName) {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(new File(fileName), true)));
			out.println(info.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}
    
    public static void test() {
    	BufferedReader reader = null;
    	int n = 0;
		try {
			String line;
			reader = new BufferedReader(new FileReader(
					new File("D:\\Eclipse\\graph_hdfs_mapreduce\\graph_mapreduce\\finalGraphFile-mapreduce.txt")));
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("Function:")) {
					n ++;
				}
				System.out.println(line);
			}
		} catch (Exception e) {
			
		}
		System.out.println(n);
    }

}

