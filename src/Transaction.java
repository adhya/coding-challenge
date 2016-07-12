import java.sql.Timestamp;

/**
 * Transaction Transfer object with Setter and getter methods
 */
public class Transaction {
    public Transaction(Timestamp created_time, String target, String actor) {
        super();
        this.created_time = created_time;
        this.target = target;
        this.actor = actor;
    }

    Timestamp created_time = null;
    String target = null;
    String actor = null;

    public Timestamp getCreated_time() {
        return created_time;
    }

    public void setCreated_time(Timestamp created_time) {
        this.created_time = created_time;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getActor() {
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

}
