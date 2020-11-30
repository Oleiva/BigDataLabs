package io.github.oleiva.analytics.model;

/*
{
   "data":{
      "id":1297851689496576,
      "id_str":"1297851689496576",
      "order_type":1,
      "datetime":"1605693298",
      "microtimestamp":"1605693298239000",
      "amount":0.014,
      "amount_str":"0.01400000",
      "price":18058.68,
      "price_str":"18058.68"
   },
   "channel":"live_orders_btcusd",
   "event":"order_deleted"
}
 */
import java.util.Objects;

public class EventData {
    private Long id;
    private int order_type;
    private String microTimeStamp;
    private double amount;
    private double price;
    private String event;

    public EventData() {
    }

    public EventData(Long id, int order_type, String microTimeStamp, double amount, double price, String event) {
        this.id = id;
        this.order_type = order_type;
        this.microTimeStamp = microTimeStamp;
        this.amount = amount;
        this.price = price;
        this.event = event;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getOrder_type() {
        return order_type;
    }

    public void setOrder_type(int order_type) {
        this.order_type = order_type;
    }

    public String getMicroTimeStamp() {
        return microTimeStamp;
    }

    public void setMicroTimeStamp(String microTimeStamp) {
        this.microTimeStamp = microTimeStamp;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public String toString() {
        return "id : " + id + " order_type : " + order_type + " microTimeStamp : " + microTimeStamp + " amount : " + amount + " price : " + price + " event : " + event;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventData eventData = (EventData) o;
        return order_type == eventData.order_type &&
                Double.compare(eventData.amount, amount) == 0 &&
                Double.compare(eventData.price, price) == 0 &&
                Objects.equals(id, eventData.id) &&
                Objects.equals(microTimeStamp, eventData.microTimeStamp) &&
                Objects.equals(event, eventData.event);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, order_type, microTimeStamp, amount, price, event);
    }
}
