package com.llf.myhbase.org.apache.hadoop.hbase.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectionUtils {
    public static <T> T instantiateWithCustomCtor(String className,
                                                  Class<? >[] ctorArgTypes, Object[] ctorArgs) {
        try {
            Class<? extends T> resultType = (Class<? extends T>) Class.forName(className);
            Constructor<? extends T> ctor = resultType.getDeclaredConstructor(ctorArgTypes);
            return instantiate(className, ctor, ctorArgs);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException(
                    "Unable to find " + className, e);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException(
                    "Unable to find suitable constructor for class " + className, e);
        }
    }

    private static <T> T instantiate(final String className, Constructor<T> ctor, Object[] ctorArgs) {
        try {
            return ctor.newInstance(ctorArgs);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException(
                    "Unable to access specified class " + className, e);
        } catch (InstantiationException e) {
            throw new UnsupportedOperationException(
                    "Unable to instantiate specified class " + className, e);
        } catch (InvocationTargetException e) {
            throw new UnsupportedOperationException(
                    "Constructor threw an exception for " + className, e);
        }
    }

    public static <T> T newInstance(Class<T> type, Object... params) {
        return instantiate(type.getName(), findConstructor(type, params), params);
    }

    public static <T> Constructor<T> findConstructor(Class<T> type, Object... paramTypes) {
        Constructor<T>[] constructors = (Constructor<T>[])type.getConstructors();
        for (Constructor<T> ctor : constructors) {
            Class<?>[] ctorParamTypes = ctor.getParameterTypes();
            if (ctorParamTypes.length != paramTypes.length) {
                continue;
            }

            boolean match = true;
            for (int i = 0; i < ctorParamTypes.length && match; ++i) {
                Class<?> paramType = paramTypes[i].getClass();
                match = (!ctorParamTypes[i].isPrimitive()) ? ctorParamTypes[i].isAssignableFrom(paramType) :
                        ((int.class.equals(ctorParamTypes[i]) && Integer.class.equals(paramType)) ||
                                (long.class.equals(ctorParamTypes[i]) && Long.class.equals(paramType)) ||
                                (char.class.equals(ctorParamTypes[i]) && Character.class.equals(paramType)) ||
                                (short.class.equals(ctorParamTypes[i]) && Short.class.equals(paramType)) ||
                                (boolean.class.equals(ctorParamTypes[i]) && Boolean.class.equals(paramType)) ||
                                (byte.class.equals(ctorParamTypes[i]) && Byte.class.equals(paramType)));
            }

            if (match) {
                return ctor;
            }
        }
        throw new UnsupportedOperationException(
                "Unable to find suitable constructor for class " + type.getName());
    }
}
