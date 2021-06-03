package org.apache.flume.sink.hbase;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import com.google.common.base.Charsets;

public class DsjAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer{
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DsjAsyncHbaseEventSerializer.class);
	private byte[] table;
	  private byte[] cf;
	  private byte[] payload;
	  private byte[] payloadColumn;
	  private byte[] incrementColumn;
	  private String rowPrefix;
	  private byte[] incrementRow;
	  private KeyType keyType;

	  @Override
	  public void initialize(byte[] table, byte[] cf) {
	    this.table = table;
	    this.cf = cf;
	  }

	  @Override
	  public List<PutRequest> getActions() {
	    List<PutRequest> actions = new ArrayList<PutRequest>();
	    if (payloadColumn != null) {
	      byte[] rowKey;
	      try {
	    	//获取每行的值
	        String[] columns = new String(this.payloadColumn).split(",");
	        //获取每列的值
	        String[] values = new String(this.payload).split(",");
	        //过滤不正确的数据
	        if(columns.length != values.length){
	        	return actions;
	        }
	        //取出数据并定义rowKey
	        String logtime = values[0].toString();
	        String uid = values[1].toString();
	        rowKey = SimpleRowKeyGenerator.getDsjRowKey(uid, logtime);
	        ///logger.info("rowKey="+rowKey);
	        //向HBase循环插入数据
	        for(int i=0;i<columns.length;i++){
	        	byte[] colColumn = columns[i].getBytes();
	        	byte[] colValue = values[i].getBytes(Charsets.UTF_8);
	        	PutRequest putRequest =  new PutRequest(table, rowKey, cf,
	        			colColumn, colValue);
	    	    actions.add(putRequest);
	    	    //logger.info("插入数据"+i);
	        }
	        
	      } catch (Exception e) {
	        throw new FlumeException("Could not get row key!", e);
	      }
	    }
	    return actions;
	  }

	  public List<AtomicIncrementRequest> getIncrements() {
	    List<AtomicIncrementRequest> actions = new ArrayList<AtomicIncrementRequest>();
	    if (incrementColumn != null) {
	      AtomicIncrementRequest inc = new AtomicIncrementRequest(table,
	          incrementRow, cf, incrementColumn);
	      actions.add(inc);
	    }
	    return actions;
	  }

	  @Override
	  public void cleanUp() {
	    // TODO Auto-generated method stub

	  }

	  @Override
	  public void configure(Context context) {
	    String pCol = context.getString("payloadColumn", "pCol");
	    String iCol = context.getString("incrementColumn", "iCol");
	    rowPrefix = context.getString("rowPrefix", "default");
	    String suffix = context.getString("suffix", "uuid");
	    if (pCol != null && !pCol.isEmpty()) {
	      if (suffix.equals("timestamp")) {
	        keyType = KeyType.TS;
	      } else if (suffix.equals("random")) {
	        keyType = KeyType.RANDOM;
	      } else if (suffix.equals("nano")) {
	        keyType = KeyType.TSNANO;
	      } else {
	        keyType = KeyType.UUID;
	      }
	      payloadColumn = pCol.getBytes(Charsets.UTF_8);
	    }
	    if (iCol != null && !iCol.isEmpty()) {
	      incrementColumn = iCol.getBytes(Charsets.UTF_8);
	    }
	    incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
	  }

	  @Override
	  public void setEvent(Event event) {
	    this.payload = event.getBody();
	  }

	  @Override
	  public void configure(ComponentConfiguration conf) {
	    // TODO Auto-generated method stub
	  }
}
