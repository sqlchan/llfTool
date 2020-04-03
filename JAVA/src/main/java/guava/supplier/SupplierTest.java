package guava.supplier;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;


public class SupplierTest {

    Supplier<User> userSupplier = Suppliers.memoize(() -> new User("llf"));
    public  User getSingle(){
        return userSupplier.get();
    }
}
