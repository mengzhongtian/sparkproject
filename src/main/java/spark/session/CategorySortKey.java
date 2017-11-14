package spark.session;

import scala.Serializable;
import scala.math.Ordered;

public class CategorySortKey implements Ordered<CategorySortKey>, Serializable{
    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public int compare(CategorySortKey that) {
        if(clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if(orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if(payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }

    public boolean $less(CategorySortKey that) {
        if(clickCount < that.getClickCount()) {
            return true;
        } else if(clickCount == that.getClickCount() &&
                orderCount < that.getOrderCount()) {
            return true;
        } else if(clickCount == that.getClickCount() &&
                orderCount == that.getOrderCount() &&
                payCount < that.getPayCount()) {
            return true;
        }
        return false;
    }

    public boolean $greater(CategorySortKey that) {
        if (clickCount > that.clickCount) {
            return true;
        } else if (clickCount == that.clickCount) {
            if (orderCount > that.orderCount) {
                return true;

            } else if (orderCount == that.orderCount) {
                if (payCount > payCount) {
                    return true;
                } else {
                    return false;
                }


            } else {
                return false;
            }
        } else {
            return false;
        }

    }

    public boolean $less$eq(CategorySortKey that) {
        if($less(that)) {
            return true;
        } else if(clickCount == that.getClickCount() &&
                orderCount == that.getOrderCount() &&
                payCount == that.getPayCount()) {
            return true;
        }
        return false;


    }

    public boolean $greater$eq(CategorySortKey that) {
        boolean b = $greater(that);
        if (b) {
            return true;
        } else {
            if (orderCount == that.orderCount && payCount == that.payCount && clickCount == that.clickCount) {
                 return true;
            } else {
                return false;
            }

        }
    }

    public int compareTo(CategorySortKey that) {
        if(clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if(orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if(payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }


    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
