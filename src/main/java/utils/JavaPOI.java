package utils;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author ss
 */
public class JavaPOI {
    public static void main(String[] args) throws IOException {
        // 文件路径
        String filePath = "/Users/saicao/Desktop/test11.xls";

        // 创建Excel
        HSSFWorkbook hssfWorkbook = new HSSFWorkbook();
        HSSFSheet sheet = hssfWorkbook.createSheet("测试");

        FileOutputStream fos = new FileOutputStream(filePath);

        hssfWorkbook.write(fos);

        fos.close();

        System.out.println("ok");

    }


}
