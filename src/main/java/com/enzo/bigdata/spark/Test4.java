package com.enzo.bigdata.spark;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Test4 {
    public static void main(String[] args) {

        List<String> names = Arrays.asList(
                "zhangsan", "lisi", "wangwu"
        );

        // TODO 函数式编程

//        names.forEach((String s) -> {
//            System.out.println(s);
//        });
        // TODO 参数类型可以省略
//        names.forEach((a) -> {
//            System.out.println(a);
//        });
        // TODO 如果参数列表中的参数只有一个，那么小括号可以省略
//        names.forEach(a -> {
//            System.out.println(a);
//        });

        // TODO 如果逻辑代码只有一行，那么大括号可以省略，而且可以同时省略分号
        //names.forEach(a -> System.out.println(a));

        // TODO 如果参数在逻辑代码中只使用了一次，那么参数可以省略,但是需要采用特殊方式访问
        //names.forEach(System.out::println);

        List<Integer> nums = Arrays.asList(
                1, 2, 3, 4, 5, 6
        );

        // 所有的计算都是两两计算
        final Optional<Integer> reduce = nums.stream().reduce(
                Integer::sum
        );
        System.out.println(reduce.get());
    }

    @Test
    public void test() {
        String words = "enzo enzo";

        String[] temp = words.split(" ");
        StringBuffer stringBuffer = new StringBuffer();
        for (String s : temp) {
            stringBuffer.append(s.substring(0, 1).toUpperCase()).append(s.substring(1));
        }

        System.out.println(stringBuffer);
    }
}
