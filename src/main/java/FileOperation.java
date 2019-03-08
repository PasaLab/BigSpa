import java.io.File;

/**
 * Created by cycy on 2019/1/5.
 */
public class FileOperation {
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                System.out.println("delete single file" + fileName + "success!");
                return true;
            } else {
                System.out.println("delete single file" + fileName + "failed!");
                return false;
            }
        } else {
            System.out.println("delete single file failed , " + fileName + "not exist!");
            return false;
        }
    }
    public static void RenameFile(String src,String dst) {
        try {
            File srcfile = new File(src);
            File desfile = new File(dst);
            if (desfile.exists()) {
                boolean res = desfile.delete();
                if (!res) {
                    System.out.println("Failed to delete file");
                }
            }
            if (!srcfile.renameTo(desfile)) {
                System.out.println("Failed to renameTo file");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
