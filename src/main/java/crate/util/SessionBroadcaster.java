package crate.util;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class SessionBroadcaster {
    public static <T extends Serializable> Broadcast<T> broadcast(SparkSession session, T object) {
        return session.sparkContext().broadcast(object, scala.reflect.ClassTag$.MODULE$.apply(object.getClass()));
    }
}