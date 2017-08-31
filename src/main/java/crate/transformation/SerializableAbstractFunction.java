package crate.transformation;

import scala.Serializable;
import scala.runtime.AbstractFunction1;

public abstract class SerializableAbstractFunction<T, R> extends AbstractFunction1<T, R> implements Serializable {
}
