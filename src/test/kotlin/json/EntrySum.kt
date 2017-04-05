package json

/**
 * Created by diegoicosta on 28/02/17.
 */

data class EntrySum(val account: String, var balance: Long) {


    public fun add(increment: Long) {
        balance += increment
    }
}