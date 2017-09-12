package crate.util;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;

import static crate.meta.AppMetadata.*;

/**
 * Allows serializable distributed Spark objects to be stored/fetched in/from CrateDB BLOB tables.
 *
 * IMPORTANT: When saving distributed Spark objects, make sure the object is broadcasted.
 * This way the object is fully available on every node. See example below:
 * Broadcast<PipelineModel> modelBroadcast = SessionBroadcaster.broadcast(session, model);
 * save(properties, MODEL_NAME, modelBroadcast.getValue())
 */
public final class CrateBlobRepository {

    private final static CloseableHttpClient httpClient = HttpClients.createDefault();
    private final static char[] hexArray = "0123456789abcdef".toCharArray();

    private static boolean isSetUp = false;

    public static void save(Properties properties, String name, Object object) throws SQLException, IOException {
        try (Connection c = DriverManager.getConnection(properties.getProperty(CRATE_JDBC_CONNECTION_URL), properties.getProperty(CRATE_USER), "")) {
            setupTables(c);

            byte[] data = toByteArray(object);
            String digest = digestOf(data);

            String urlString = String.format("%s%s",
                    properties.getProperty(CRATE_BLOB_CONNECTION_URL),
                    digest
            );

            HttpPut httpPut = new HttpPut(urlString);
            httpPut.setEntity(new ByteArrayEntity(data));

            httpClient.execute(httpPut);

            PreparedStatement stmt = c.prepareStatement(String.format("insert into %s (created_at, name, digest) values (?, ?, ?)", TABLE_NAME));
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.setString(2, name);
            stmt.setString(3, digest);

            stmt.executeUpdate();
        }
    }

    public static Object load(Properties properties, String name) throws SQLException, IOException, ClassNotFoundException {
        Object object = null;
        try (Connection c = DriverManager.getConnection(properties.getProperty(CRATE_JDBC_CONNECTION_URL), properties.getProperty(CRATE_USER), "")) {
            setupTables(c);

            PreparedStatement stmt = c.prepareStatement(
                    String.format("select * from %s where name=? order by created_at desc limit 1", TABLE_NAME)
            );
            stmt.setString(1, name);

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String urlString = String.format("%s%s",
                        properties.getProperty(CRATE_BLOB_CONNECTION_URL),
                        rs.getString("digest")
                );
                // read object from url
                object = new ObjectInputStream(new URL(urlString).openStream()).readObject();
            }
        }
        return object;
    }

    public static void setupTables(Connection connection) throws SQLException {
        if (isSetUp)
            return;

        try {
            //create blob table if not exists
            connection.prepareStatement(
                    String.format("create blob table %s", TABLE_NAME)
            ).executeUpdate();
        } catch (SQLException e) {
            // do nothing - exists already
        }

        //create lookup table if not exists
        connection.prepareStatement(
                String.format("create table if not exists %s (created_at timestamp primary key, name string primary key, digest string not null)", TABLE_NAME)
        ).executeUpdate();

        isSetUp = true;
    }

    public static byte[] toByteArray(Object object) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(object);
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static String digestOf(byte[] data) {
        try {
            return toHex(MessageDigest.getInstance("SHA-1").digest(data));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    public static String toHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for (int j = 0; j < bytes.length; j++) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

}
