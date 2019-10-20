import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName Test01
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 11:18
 * @Version 1.0
 **/
public class Test01 {


    @org.junit.Test
    public void test01() {

        List<Integer> list1 = new ArrayList<>();
        List<Integer> list2 = new ArrayList<>();
        List<Integer> list3 = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            if (i % 3 == 0) {
                list1.add(i);
            } else if (i % 3 == 1) {
                list2.add(i);
            } else if (i % 3 == 2) {
                list3.add(i);
            }
        }
        System.out.println(list1.size()+":"+Arrays.toString(list1.toArray()));
        System.out.println(list2.size()+":"+Arrays.toString(list2.toArray()));
        System.out.println(list3.size()+":"+Arrays.toString(list3.toArray()));


    }


}
