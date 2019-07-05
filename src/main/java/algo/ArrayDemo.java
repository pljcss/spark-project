package algo;

public class ArrayDemo {
    private int[] data;
    private int n;
    private int count;

    public ArrayDemo(int capacity) {
        this.data = new int[capacity];
        this.n = capacity;
        this.count = 0;
    }


    public int find(int index) {
        if (index < 0 || index > count) {
            return -1;
        }

        return data[index];
    }

}
