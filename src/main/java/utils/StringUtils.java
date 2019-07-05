package utils;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * @author cc
 */
public class StringUtils {
    public static void main(String[] args) {
        String id_num = "320382199210026665";
        String res = org.apache.commons.lang.StringUtils.leftPad(id_num, 18, '0');

        System.out.println(res);


        String res1 = DigestUtils.md5Hex(id_num).substring(0, 1);

        System.out.println(res1);

        System.out.println(res + res1);
    }

}
