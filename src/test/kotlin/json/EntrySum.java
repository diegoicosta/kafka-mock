package json;

/**
 * Created by diegoicosta on 24/06/17.
 */
public class EntrySum {
    private String account;
    private long balance;

    public EntrySum(String account, long balance) {
        this.account = account;
        this.balance = balance;
    }

    public String getAccount() {
        return account;
    }

    public long getBalance() {
        return balance;
    }

    public void add(long increment) {
        balance += increment;
    }
}