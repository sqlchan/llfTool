package guava.supplier;


import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class SuppilerSingletonTest {
    public static void main(String[] args) {
        SupplierTest supplierTest = new SupplierTest();
        User user1 = supplierTest.getSingle();
        User user2 = supplierTest.getSingle();
        if (user1 == user2) System.out.println("=========");
    }
    class HeavyObject{
        public HeavyObject() {
            System.out.println("being created");
        }
    }

    class ObjectSuppiler implements Supplier<HeavyObject> {

        @Override
        public HeavyObject get() {
            return new HeavyObject();
        }
    }

    /**
     * 每次都new一次
     */
    public void testNotSingleton(){
        Supplier<HeavyObject> notCached = new ObjectSuppiler();
        for(int i=0;i<5;i++){
            notCached.get();
        }
    }

    /**
     * 单例
     */
    public void testSingleton(){
        Supplier<HeavyObject> notCached = new ObjectSuppiler();
        Supplier<HeavyObject> cachedSupplier = Suppliers.memoize(notCached);
        for(int i=0;i<5;i++){
            cachedSupplier.get();
        }
    }
}
