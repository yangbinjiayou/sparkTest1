import java.text.SimpleDateFormat;

public class Test {
    public static void main(String[] args) {
        String yyMMddhhmmss = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss").format(Long.parseLong("1516609143867"));
        System.out.println(yyMMddhhmmss);
    }
}
