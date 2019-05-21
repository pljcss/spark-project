package alg;

public class BinarySearch {

    public static void main(String[] args) {
        int[] array = new int[]{1,2,3,4,5,6,7,8,9};


        int a = search(array, 9);

        System.out.println(a);


    }


    /**
     * 二分查找
     * @param array 输入数组
     * @param b 待查找数字
     * @return
     */
    private static int binarySearch(int[] array, int b) {

        int low = 0;
        int high = array.length-1;
        int middle;

        while (low <= high) {
            middle = (low + high) / 2;

            if (array[middle] == b){
                return array[middle];
            }

            if (array[middle] > b) {
                high = middle - 1;
            }

            if (array[middle] < b) {
                low = middle + 1;
            }

        }
        return -1;
    }

    /**
     * 顺序查找
     * @param array
     * @param b
     * @return
     */
    private static int search(int[] array, int b) {

        for (int i = 0; i<=array.length-1; i++) {
            if (array[i] == b) {
                return array[i];
            }
        }

        return -1;
    }

}
