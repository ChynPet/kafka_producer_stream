import entities.User;
import kafka.Producer;

import java.sql.Time;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class Runner {
    public static void main (String argv[]) throws Exception{
        // Simple test producer
        Producer producer = new Producer("input", 1);

        User user1 = new User("user1", "1234");
        User user2 = new User("user2", "1234");
        User user3 = new User("user3", "1234");

        for (int i = 0; i < 100; i++){
            producer.send(user1);
        }

        for(int i = 0; i < 9; i++){
            producer.send(user2);
        }

        for(int i = 0; i < 1000; i++){
            producer.send(user3);
        }

        producer.close();
    }
}
