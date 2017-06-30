package json;

/**
 * Created by diegoicosta on 24/06/17.
 */
public class Entry {

    private String id;
    private String account;
    private long amount;
    private long fee;

    public Entry(String id, String account, long amount, long fee) {
        this.id = id;
        this.account = account;
        this.amount = amount;
        this.fee = fee;
    }

    public String getId() {
        return id;
    }

    public String getAccount() {
        return account;
    }

    public long getAmount() {
        return amount;
    }

    public long getFee() {
        return fee;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Entry entry = (Entry) o;

        if (amount != entry.amount)
            return false;
        if (fee != entry.fee)
            return false;
        if (!id.equals(entry.id))
            return false;
        return account.equals(entry.account);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + account.hashCode();
        result = 31 * result + (int) (amount ^ (amount >>> 32));
        result = 31 * result + (int) (fee ^ (fee >>> 32));
        return result;
    }
}
