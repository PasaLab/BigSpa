package utils;

/**
 * Created by cycy on 2018/3/26.
 */
public class Singleton{
    //类加载时就初始化
    private static final String name ="Singleton";

    private Singleton(){}
    public static String getname(){
        return name;
    }
}