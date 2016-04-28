package org.apache.flume.sink.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.source.SyslogUDPSource.syslogHandler;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.htrace.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import org.apache.log4j.Logger;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

/**
 * A serializer for the AsyncHBaseSink, which splits the event body into
 * multiple columns and inserts them into a row whose key is available in the
 * headers
 */
public class DvSplittingAsyncHbaseEventSerializer implements
		DvAsyncHbaseEventSerializer {
	static Logger LOG = Logger
			.getLogger(DvSplittingAsyncHbaseEventSerializer.class.getName());
	private Event currentEvent;
	private String tableName;
	private byte[] colFams;
	private Event currentEvents;
	private byte[][] columnNames;
	private final List<List<PutRequest>> puts = new ArrayList<List<PutRequest>>();
	private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
	private byte[] currentRowKeys;
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
		currentEvent = event;
		String colFam = currentEvent.getHeaders().get("family");
		colFams  = colFam.getBytes();
		String rowKeyStr = currentEvent.getHeaders().get("rowKey");
		if (rowKeyStr == null) {
			throw new FlumeException("No row key found in headers!");
		}
		currentRowKeys = rowKeyStr.getBytes();

		String cols = new String(currentEvent.getHeaders().get("columns"));

		String[] names = cols.split(",");
		byte[][] columnNames = new byte[names.length][];
		int i = 0;
		for (String name : names) {
			columnNames[i++] = name.getBytes();
		}
		this.columnNames = columnNames;
	}

	@Override
	public List<List<PutRequest>> getActions() {
		// Split the event body and get the values for the columns
		String eventStr = new String(currentEvent.getBody());
		
		String[] lines = eventStr.split("\r\n");
		List<PutRequest> tmpPuts = new ArrayList<PutRequest>();
		puts.clear();
		int len = lines.length;
		for(int l=0;l<len;l++){
			String[] cols = lines[l].split(",");
			Random random = new Random();
			// int s = random.nextInt(1000)%(1000-10+1) + 10;
			int s = random.nextInt(10000000);
			long nanoTime = System.nanoTime();
			
			String rowkey = System.nanoTime()+"_" + s;
			
			for(int i = 0; i < cols.length; i++){
				String columnName = new String(columnNames[i]);
				if(columnName.lastIndexOf(":ts")>0){
					rowkey = TimeUtil.toTime(Long.valueOf(cols[i]))+":"+rowkey;
					break;
				}
			}
			
			for (int i = 0; i < cols.length; i++) {
				PutRequest req = new PutRequest(tableName.getBytes(),
						rowkey.getBytes(), colFams,
						columnNames[i], cols[i].getBytes());
				
				tmpPuts.add(req);
			}
			puts.add(tmpPuts);
		}
		
		return puts;
	}

	@Override
	public List<AtomicIncrementRequest> getIncrements() {
		incs.clear();
		incs.add(new AtomicIncrementRequest(tableName.getBytes(), "totalEvents"
				.getBytes(), colFams, eventCountCol));
		
		return incs;
	}

	@Override
	public void cleanUp() {
		tableName = null;
		colFams = null;
		currentEvents = null;
		columnNames = null;
		currentRowKeys = null;
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
