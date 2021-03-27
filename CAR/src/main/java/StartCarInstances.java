
public class StartCarInstances {
    private static final int numberOfInstances = 1;

    public static void main(String[] args) {

        for(int i = 1; i <= numberOfInstances; i++) {
            Thread t = new Thread(Car::new);
            t.start();
        }
    }
}
