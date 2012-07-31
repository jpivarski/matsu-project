package org.occ.matsu.accumulo;

import java.util.Set;
import java.util.Iterator;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.DataOutputStream;
import java.io.FileOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.Connector;

import org.apache.hadoop.io.Text;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import java.io.IOException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import org.json.simple.JSONValue;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

public class AccumuloInterface {
    public static void main(String argv[]) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException, TableExistsException, IOException {
	if (argv.length != 2) {
	    throw new RuntimeException("Pass a command: 'read TABLENAME' or 'write TABLENAME'.");
	}

	if (argv[0].equals("read")) {
	    String tableName = argv[1];
	    System.out.println("AccumuloInterface reading from " + tableName);

	    Instance zookeeper = new ZooKeeperInstance("accumulo", "192.168.18.101:2181");
	    if (zookeeper == null) {
		System.err.println("ZooKeeper instance not found");
	    }

	    Connector connector = zookeeper.getConnector("root", "password".getBytes());
	    Scanner scan = connector.createScanner(tableName, Constants.NO_AUTHS);

	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
	    String line;
	    while ((line = bufferedReader.readLine()) != null) {
		scan.setRange(new Range(line));

		int count = 0;
		String output = "{";
		for (Entry<Key, Value> entry : scan) {
		    if (count > 0) {
			output += ",";
		    }

		    String columnName = entry.getKey().getColumnQualifier().toString();

		    if (columnName.equals("L2PNG")) {
			output += "\"" + JSONObject.escape(columnName) + "\":\"" + Base64.encodeBase64String(entry.getValue().get()) + "\"";
		    }
		    else {
			output += "\"" + JSONObject.escape(columnName) + "\":" + entry.getValue().toString();
		    }

		    count++;
		}
		output += "}";

		if (count == 0) {
		    System.out.println("None");
		}
		else {
		    System.out.println(output);
		}

	    }
	}
	else if (argv[0].equals("image")) {
	    String tableName = argv[1];

	    Instance zookeeper = new ZooKeeperInstance("accumulo", "192.168.18.101:2181");
	    if (zookeeper == null) {
		System.err.println("ZooKeeper instance not found");
	    }

	    Connector connector = zookeeper.getConnector("root", "password".getBytes());
	    Scanner scan = connector.createScanner(tableName, Constants.NO_AUTHS);

	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
	    String line;
	    while ((line = bufferedReader.readLine()) != null) {
		scan.setRange(new Range(line));

		for (Entry<Key, Value> entry : scan) {
		    DataOutputStream output = new DataOutputStream(new FileOutputStream(line + ".png"));
		    if (entry.getKey().getColumnQualifier().toString().equals("L2PNG")) {
			entry.getValue().write(output);
		    }
		    output.close();
		}
	    }
	}
	else if (argv[0].equals("write")) {
	    String tableName = argv[1];
	    System.out.println("AccumuloInterface writing to " + tableName);

	    Instance zookeeper = new ZooKeeperInstance("accumulo", "192.168.18.101:2181");
	    if (zookeeper == null) {
		System.err.println("ZooKeeper instance not found");
	    }

	    Connector connector = zookeeper.getConnector("root", "password".getBytes());

	    if (!connector.tableOperations().exists(tableName)) {
		connector.tableOperations().create(tableName);
	    }
	
	    MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(200000L, 300, 4);
	    BatchWriter batchWriter = multiTableBatchWriter.getBatchWriter(tableName);

	    Text columnFamily = new Text("f");

	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
	    String line;
	    while ((line = bufferedReader.readLine()) != null) {
		JSONObject obj = (JSONObject)JSONValue.parse(line);
		if (obj == null) {
		    System.out.println("Received a malformated JSON object; skipping...");
		    continue;
		}

		String key = (String)obj.get("KEY");
		if (key == null) {
		    System.out.println("JSON object is missing its 'KEY'; skipping...");
		    continue;
		}

		String l2png = (String)obj.get("L2PNG");
		if (l2png == null) {
		    System.out.println("JSON object is missing its 'L2PNG'; skipping...");
		    continue;
		}

		Set columnNames = obj.keySet();
		columnNames.remove(new String("KEY"));
		columnNames.remove(new String("L2PNG"));

		Mutation mutation = new Mutation(new Text(key));

 		mutation.put(columnFamily, new Text("L2PNG"), new Value(StringUtils.newStringUtf8(Base64.decodeBase64(l2png)).getBytes()));

		for (Object columnName : columnNames) {
		    Object value = obj.get(columnName);

		    try {
			String stringValue = (String)value;
			stringValue = "\"" + JSONObject.escape(stringValue) + "\"";
			mutation.put(columnFamily, new Text((String)columnName), new Value(stringValue.getBytes()));
		    }
		    catch (ClassCastException e) {
			mutation.put(columnFamily, new Text((String)columnName), new Value(value.toString().getBytes()));
		    }
		}

		batchWriter.addMutation(mutation);

	    }

	    multiTableBatchWriter.close();
	}
	else {
	    throw new RuntimeException("Unrecognized command: must be 'read' or 'write'.");
	}
    }
}
