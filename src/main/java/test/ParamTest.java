package test;

/**
 * @author css
 */
public class ParamTest {
    public void changePoint(Point point) {
        point = new Point();

        point.x = 3;
        point.y = 4;
    }

    public static void main(String[] args) {
        ParamTest paramTest = new ParamTest();
        Point point = new Point();

        paramTest.changePoint(point);

        System.out.println(point.x);
        System.out.println(point.y);

    }
}

class Point {
    int x;
    int y;
}
