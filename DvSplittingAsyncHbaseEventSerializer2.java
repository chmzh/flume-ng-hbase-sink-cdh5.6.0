package org.apache.flume.sink.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.log4j.Logger;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

/**
 * A serializer for the AsyncHBaseSink, which splits the event body into
 * multiple columns and inserts them into a row whose key is available in the
 * headers
 */
public class DvSplittingAsyncHbaseEventSerializer2 implements
		AsyncHbaseEventSerializer {
	static Logger LOG = Logger
			.getLogger(DvSplittingAsyncHbaseEventSerializer2.class.getName());
	//private Event currentEvent;
	private String tableName;
	private ConcurrentMap<String, byte[]> colFams = new ConcurrentHashMap<String, byte[]>();
	private ConcurrentMap<String, Event> currentEvents = new ConcurrentHashMap<String, Event>();
	private ConcurrentMap<String, byte[][]> columnNames = new ConcurrentHashMap<String, byte[][]>();
	private final ConcurrentMap<String, List<PutRequest>> puts = new ConcurrentHashMap<String, List<PutRequest>>();
	private final ConcurrentMap<String, List<AtomicIncrementRequest>> incs = new ConcurrentHashMap<String, List<AtomicIncrementRequest>>();
	private ConcurrentMap<String, byte[]> currentRowKeys = new ConcurrentHashMap<String, byte[]>();
	private final byte[] eventCountCol = "eventCount".getBytes();

	@Override
	public void initialize(byte[] table, byte[] cf) {
		// this.table = table;
		// this.colFam = cf;
	}

	@Override
	public void setEvent(Event event) {
		// Set the event and verify that the rowKey is not present
		tableName = event.getHeaders().get("table");
		this.currentEvents.putIfAbsent(tableName, event);
		Event currentEvent = this.currentEvents.get(tableName);
		String colFam = currentEvent.getHeaders().get("family");
		this.colFams.putIfAbsent(tableName, colFam.getBytes());
		String rowKeyStr = currentEvent.getHeaders().get("rowKey");
		if (rowKeyStr == null) {
			throw new FlumeException("No row key found in headers!");
		}

		currentRowKeys.put(tableName, rowKeyStr.getBytes());

		String cols = new String(currentEvent.getHeaders().get("columns"));
		//LOG.info("colums:" + cols);
		String[] names = cols.split(",");
		byte[][] columnNames = new byte[names.length][];
		int i = 0;
		for (String name : names) {
			columnNames[i++] = name.getBytes();
		}
		this.columnNames.put(tableName, columnNames);
	}

	@Override
	public List<PutRequest> getActions() {
		// Split the event body and get the values for the columns

		String eventStr = new String(this.currentEvents.get(tableName).getBody());
		String[] cols = eventStr.split(",");
		LOG.error(tableName+":"+eventStr);
		puts.clear();
		List<PutRequest> requests = new ArrayList<PutRequest>();
		for (int i = 0; i < cols.length; i++) {
			// Generate a PutRequest for each column.
			LOG.info(tableName+"|||"
					+new String(currentRowKeys.get(tableName))
					+"|||"
					+new String(colFams.get(tableName))
					+"|||"
					+new String(columnNames.get(tableName)[i])
					+"|||"
					+cols[i]);
			PutRequest req = new PutRequest(tableName.getBytes(),
					currentRowKeys.get(tableName), colFams.get(tableName),
					columnNames.get(tableName)[i], cols[i].getBytes());

			requests.add(req);
		}
		puts.putIfAbsent(tableName, requests);
		return puts.get(tableName);
	}

	@Override
	public List<AtomicIncrementRequest> getIncrements() {
		if(incs.get(tableName)!=null){
			incs.get(tableName).clear();
			// Increment the number of events received
			incs.get(tableName).add(
					new AtomicIncrementRequest(tableName.getBytes(), "totalEvents"
							.getBytes(), colFams.get(tableName), eventCountCol));
		}else{
			List<AtomicIncrementRequest> inc = new ArrayList<AtomicIncrementRequest>();
			inc.add(new AtomicIncrementRequest(tableName.getBytes(), "totalEvents"
							.getBytes(), colFams.get(tableName), eventCountCol));
			incs.put(tableName, inc);
		}

		return incs.get(tableName);
	}

	@Override
	public void cleanUp() {
		tableName = null;
		colFams = new ConcurrentHashMap<String, byte[]>();
		currentEvents = new ConcurrentHashMap<String, Event>();
		columnNames = new ConcurrentHashMap<String, byte[][]>();
		currentRowKeys = new ConcurrentHashMap<String, byte[]>();
	}

	@Override
	public void configure(Context context) {
		// Get the column names from the configuration
		// String cols = new String(context.getString("columns"));
		// LOG.error("colums:"+cols);
		// String[] names = cols.split(",");
		// byte[][] columnNames = new byte[names.length][];
		// int i = 0;
		// for(String name : names) {
		// columnNames[i++] = name.getBytes();
		// }
		// this.columnNames = columnNames;
	}

	@Override
	public void configure(ComponentConfiguration conf) {

	}
}
