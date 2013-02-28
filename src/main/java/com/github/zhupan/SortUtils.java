package com.github.zhupan;

/**
 * @author PanosZhu
 */
public abstract class SortUtils {

    public static Integer[] sort(Integer[] array) {
        for (int i = 1; i < array.length; i++) {
            Integer key = array[i];
            int j = i - 1;
            while (j >= 0 && array[j] > key) {
                array[j + 1] = array[j];
                j = j - 1;
            }
            array[j + 1] = key;
        }
        return array;
    }
}
