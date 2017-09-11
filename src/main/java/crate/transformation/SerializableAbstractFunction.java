package crate.transformation;

import scala.Serializable;
import scala.runtime.AbstractFunction1;

/**
 * Simplifies the implementation of AbstractFunction1 in Java
 * @param <T> type that goes in as parameter
 * @param <R> type that goes out as return type
 */
public abstract class SerializableAbstractFunction<T, R> extends AbstractFunction1<T, R> implements Serializable {
}
