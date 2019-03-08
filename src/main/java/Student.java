import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by cycy on 2019/3/3.
 */

public class Student {
    private int id;
    private String  name;
    private String sex;
    private ArrayList<String> array;

    public Student(){
        id=-1;
        name="null";
        sex="female";
        array=new ArrayList<>();
    }
    public Student(int _id,String _name,ArrayList<String> _array){
        id=_id;
        name=_name;
        array=_array;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public ArrayList<String> getArray() {
        return array;
    }

    public void setArray(ArrayList<String> array) {
        this.array = array;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", array=" + Arrays.toString(array.toArray()) +
                '}';
    }
}
