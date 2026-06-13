public class Order {

    private String orderId;
    private String product;
    private int quantity;

    public Order(
            String orderId,
            String product,
            int quantity
    ) {
        this.orderId = orderId;
        this.product = product;
        this.quantity = quantity;
    }

    @Override
    public String toString() {

        return orderId +
                "," +
                product +
                "," +
                quantity;
    }
}