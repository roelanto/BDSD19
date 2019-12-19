package nl.hu;

/**
 * Created by roelant on 19/12/2019.
 */
public class Transaction {
    int customerId;
    int productId;


    public Transaction(int customerId, int productId) {
        this.customerId = customerId;
        this.productId = productId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public int getProductId() {
        return productId;
    }


    @Override
    public String toString() {
        return "Transaction{" +
                "customerId=" + customerId +
                ", productId=" + productId +
                '}';
    }
}
