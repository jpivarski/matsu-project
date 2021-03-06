package org.occ.matsu;

import org.occ.matsu.LngLat;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.Connector;

import org.apache.hadoop.io.Text;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

public class AccumuloInterface {
    static Instance zooKeeperInstance = null;
    static Connector connector = null;
    static String tableName = null;

    static Scanner scanner = null;

    static Text columnFamily = new Text("f");
    static MultiTableBatchWriter multiTableBatchWriter = null;
    static BatchWriter batchWriter = null;

    public static void connectForReading(String accumuloName, String zooKeeperList, String userId, String password, String tableName_) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
	zooKeeperInstance = new ZooKeeperInstance(accumuloName, zooKeeperList);
	if (zooKeeperInstance == null) {
	    throw new AccumuloException("Could not connect to ZooKeeper " + accumuloName + " " + zooKeeperList);
	}

	if (connector == null) {
	    connector = zooKeeperInstance.getConnector(userId, password.getBytes());
	}
	tableName = tableName_;
	scanner = connector.createScanner(tableName_, Constants.NO_AUTHS);
    }

    public static ArrayList<String> getKeys(String low, String high) throws IOException {
        scanner.setRange(new Range(low, high));
        
        ArrayList<String> output = new ArrayList<String>();
        String lastrow = null;
        for (Entry<Key, Value> entry : scanner) {
            String entryrow = entry.getKey().getRow().toString();
            if (lastrow == null  ||  !entryrow.equals(lastrow)) {
                output.add(entryrow);
            }
            lastrow = entryrow;
        }

        return output;
    }

    public static byte[] readL2png(String key) throws IOException {
	scanner.setRange(new Range(key));

	for (Entry<Key, Value> entry : scanner) {
	    String columnName = entry.getKey().getColumnQualifier().toString();
	    if (columnName.equals("l2png")) {
	    	return entry.getValue().get();
	    }
	}

	throw new IOException("key \"" + key + "\" column \"l2png\" not found");
    }

    public static String readMetadata(String key) throws IOException {
	scanner.setRange(new Range(key));

	for (Entry<Key, Value> entry : scanner) {
	    String columnName = entry.getKey().getColumnQualifier().toString();
	    if (columnName.equals("metadata")) {
	    	return entry.getValue().toString();
	    }
	}

	throw new IOException("key \"" + key + "\" column \"l2png\" not found");
    }

    public static ArrayList<LngLat> lnglat_read(String tileName) {
	return lnglat_read(tileName, 0L, 9999999999L);
    }

    public static ArrayList<LngLat> lnglat_read(String tileName, long minTime, long maxTime) {
	try {
	    scanner.setRange(new Range(String.format("%s-%010d", tileName, minTime), String.format("%s-%010d", tileName, maxTime)));
	}
	catch (IllegalArgumentException exception) {
	    return new ArrayList<LngLat>();
	}

	ArrayList<LngLat> output = new ArrayList<LngLat>();
	Entry<Key, Value> last = null;
	String lastrow = "";
	double longitude = -1000.0;
	double latitude = -1000.0;
	String metadata = "{}";

	for (Entry<Key, Value> entry : scanner) {
	    String entryrow = entry.getKey().getRow().toString();

	    if (last != null  &&  !lastrow.equals(entryrow)) {
		String tileName_ = lastrow.substring(0, 15);
		String timeStamp_ = lastrow.substring(16, 26);
		String identifier_ = lastrow.substring(27, 43);
		output.add(new LngLat(tileName_, timeStamp_, identifier_, longitude, latitude, metadata));

		longitude = -1000.0;
		latitude = -1000.0;
		metadata = "{}";
	    }

	    String columnName = entry.getKey().getColumnQualifier().toString();
	    if (columnName.equals("longitude")) {
		longitude = ByteBuffer.wrap(entry.getValue().get()).getDouble();
	    }
	    else if (columnName.equals("latitude")) {
		latitude = ByteBuffer.wrap(entry.getValue().get()).getDouble();
	    }
	    else if (columnName.equals("metadata")) {
		metadata = entry.getValue().toString();
	    }

	    last = entry;
	    lastrow = entryrow;
	}

	if (last != null) {
	    String tileName_ = lastrow.substring(0, 15);
	    String timeStamp_ = lastrow.substring(16, 26);
	    String identifier_ = lastrow.substring(27, 43);
	    output.add(new LngLat(tileName_, timeStamp_, identifier_, longitude, latitude, metadata));
	}
	
	return output;
    }

    public static void connectForWriting(String accumuloName, String zooKeeperList, String userId, String password, String tableName_) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
	zooKeeperInstance = new ZooKeeperInstance(accumuloName, zooKeeperList);
	if (zooKeeperInstance == null) {
	    throw new AccumuloException("Could not connect to ZooKeeper " + accumuloName + " " + zooKeeperList);
	}

	if (connector == null) {
	    connector = zooKeeperInstance.getConnector(userId, password.getBytes());
	}
	tableName = tableName_;
	if (!connector.tableOperations().exists(tableName_)) {
	    connector.tableOperations().create(tableName_);
	}

	multiTableBatchWriter = connector.createMultiTableBatchWriter(200000L, 300, 4);
	batchWriter = multiTableBatchWriter.getBatchWriter(tableName_);
    }

    public static void write(String key, String metadata, byte[] l2png) throws MutationsRejectedException {
	Mutation mutation = new Mutation(new Text(key));
	mutation.put(columnFamily, new Text("metadata"), new Value(metadata.getBytes()));
	mutation.put(columnFamily, new Text("l2png"), new Value(l2png));
	batchWriter.addMutation(mutation);
    }

    public static void flush() throws MutationsRejectedException {
        multiTableBatchWriter.flush();
    }

    public static void lnglat_write(String key, double longitude, double latitude, String metadata) throws MutationsRejectedException {
	Mutation mutation = new Mutation(new Text(key));
	mutation.put(columnFamily, new Text("metadata"), new Value(metadata.getBytes()));

	byte[] longitudeBytes = new byte[8];
	ByteBuffer.wrap(longitudeBytes).putDouble(longitude);
	mutation.put(columnFamily, new Text("longitude"), new Value(longitudeBytes));

	byte[] latitudeBytes = new byte[8];
	ByteBuffer.wrap(latitudeBytes).putDouble(latitude);
	mutation.put(columnFamily, new Text("latitude"), new Value(latitudeBytes));

	batchWriter.addMutation(mutation);
    }

    public static void polygon_write(String key, String metadata, String polygon) throws MutationsRejectedException {
	Mutation mutation = new Mutation(new Text(key));
	mutation.put(columnFamily, new Text("metadata"), new Value(metadata.getBytes()));
	mutation.put(columnFamily, new Text("polygon"), new Value(polygon.getBytes()));
	batchWriter.addMutation(mutation);
    }

    public static void delete(String start, String end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
	// Accumulo 1.4.1
	// connector.tableOperations().deleteRows(tableName, new Text(start), new Text(end));

	// Accumulo 1.3.5
	try {
	    scanner.setRange(new Range(start, end));
	}
	catch (IllegalArgumentException exception) {
	    return;
	}
	
	int count = 0;
	for (Entry<Key, Value> entry : scanner) {
	    Mutation mutation = new Mutation(entry.getKey().getRow());
	    mutation.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
	    batchWriter.addMutation(mutation);
	    count++;
	}

	if (count > 0) {
	    multiTableBatchWriter.flush();
	}
    }

    public static void finishedWriting() throws MutationsRejectedException {
	multiTableBatchWriter.close();
    }

}
