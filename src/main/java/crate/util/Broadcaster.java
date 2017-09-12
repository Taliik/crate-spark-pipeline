package crate.util;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Allows serializable objects to be stored/fetched in/from CrateDB BLOB tables.
 *
 * each table consists of two tables:
 * 1. lookup table that contains the digest of a given object name
 * 2. blob table that holds the actual object
 *
 * IMPORTANT: When saving distributed objects, make sure the object is broadcasted beforehand.
 * This way the object is fully available everywhere.
 */
public class Broadcaster {
    public static <T extends Serializable> Broadcast<T> broadcast(SparkSession session, T object) {
        return session.sparkContext().broadcast(object, scala.reflect.ClassTag$.MODULE$.apply(object.getClass()));
    }
}