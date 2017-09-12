package crate.util;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;

import static crate.meta.AppMetadata.*;

/**
 * Stores serializable objects in CrateDB BLOB tables.
 * Loads serializable objects from CrateDB BLOB tables.
 *
 * Each table consists of two tables:
 * 1. lookup table that contains the digest of a given object name
 * 2. blob table that holds the actual object
 *
 * IMPORTANT: When saving distributed objects, make sure the object is broadcasted beforehand.
 * This way the object is fully available everywhere.
 */
public final class CrateBlobStorageUtil {

    private final static CloseableHttpClient httpClient = HttpClients.createDefault();
    private final static char[] hexArray = "0123456789abcdef".toCharArray();

    public static void save(final Properties properties, final String tableName, final String name, final Serializable object) throws IOException, SQLException {
        // serialization
        final byte[] data = toByteArray(object);
        // digest calculation
        final String digest = digestOf(data);

        try (Connection connection = DriverManager.getConnection(properties.getProperty(CRATE_JDBC_CONNECTION_URL), properties.getProperty(CRATE_USER), "")) {
            // create tables if not existing already
            createTables(connection, tableName);

            // inserts a lookup entry for the object with the current timestamp so it can be accessed later
            // when loaded, the latest object of a name is picked by default
            saveLookup(connection, tableName, digest, name);
        }

        // saves the serialized object
        saveBlob(properties, tableName, digest, data);
    }

    public static Object load(final Properties properties, final String tableName, final String name) throws SQLException, IOException, ClassNotFoundException {
        String digest;

        try (Connection connection = DriverManager.getConnection(properties.getProperty(CRATE_JDBC_CONNECTION_URL), properties.getProperty(CRATE_USER), "")) {
            // lookup latest digest
            digest = lookup(connection, tableName, name);
        }

        // load blob identified by digest
        return loadBlob(properties, tableName, digest);
    }


    private static byte[] toByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(object);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private static String digestOf(byte[] data) {
        try {
            return toHex(MessageDigest.getInstance("SHA-1").digest(data));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    private static String toHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for (int j = 0; j < bytes.length; j++) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }


    private static void createTables(final Connection connection, final String tableName) throws SQLException {
        createLookupTable(connection, tableName);
        createBlobTable(connection, tableName);
    }

    private static void createLookupTable(final Connection connection, final String tableName) throws SQLException {
        //create lookup table if not exists
        connection.prepareStatement(
                String.format("create table if not exists %s (created_at timestamp primary key, name string primary key, digest string not null)", tableName)
        ).executeUpdate();
    }

    private static void createBlobTable(final Connection connection, final String tableName) {
        try {
            //create blob table if not exists
            connection.prepareStatement(
                    String.format("create blob table %s", tableName)
            ).executeUpdate();
        } catch (SQLException e) {
            // do nothing - exists already
        }
    }


    // uses http connection to save data
    private static void saveBlob(final Properties properties, final String tableName, final String digest, final byte[] data) throws IOException {
        String urlString = String.format("%s/%s/%s",
                properties.getProperty(CRATE_BLOB_CONNECTION_URL),
                tableName,
                digest
        );

        HttpPut httpPut = new HttpPut(urlString);
        httpPut.setEntity(new ByteArrayEntity(data));

        httpClient.execute(httpPut);
    }



    private static Object loadBlob(final Properties properties, final String tableName, final String digest) throws IOException, ClassNotFoundException {
        String urlString = String.format("%s/%s/%s",
                properties.getProperty(CRATE_BLOB_CONNECTION_URL),
                tableName,
                digest
        );
        // read object from url
        return new ObjectInputStream(new URL(urlString).openStream()).readObject();
    }

    private static String lookup(final Connection connection, final String tableName, final String name) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(
                String.format("select * from %s where name=? order by created_at desc limit 1", tableName)
        );
        stmt.setString(1, name);

        ResultSet rs = stmt.executeQuery();
        if(rs.next()) {
            return rs.getString("digest");
        }
        return null;
    }

    // uses jdbc connection to save lookup
    private static void saveLookup(final Connection connection, final String tableName, final String digest, final String name) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(String.format("insert into %s (created_at, digest, name) values (?, ?, ?)", tableName));
        stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        stmt.setString(2, digest);
        stmt.setString(3, name);

        stmt.executeUpdate();
    }

}
