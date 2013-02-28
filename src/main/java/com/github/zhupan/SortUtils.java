package com.github.zhupan;

import java.lang.Double;

/**
 * @author PanosZhu
 */
public abstract class SortUtils {

    public static Long[] sort(Long[] array) {
        for (int i = 1; i < array.length; i++) {
            Long key = array[i];
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
