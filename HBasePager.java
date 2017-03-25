package com.its.bigdata.common.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
@Component("hbasePager")
public class HBasePager
{
	private static Configuration configuration = null;
	//连接池
	private static HConnection conn = null;
	
	static
	{
		try
		{
			configuration = HBaseConfiguration.create();
			conn = HConnectionManager.createConnection(configuration);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 获取hbase表中所有startRow和stopRow之间的记录，一行记录是list内的一个map
	 * @param tableName 表空间+":"+表名
	 * @param pageNum 指定页数
	 * @param pageSize 每页大小
	 * @param startRow 启始行
	 * @param stopRow 终止行
	 * @return
	 */
	public List<Map<String, Object>> getRecords(String tableName,String startRow,String stopRow)
	{
		List<Map<String, Object>> recordsList = new ArrayList<Map<String, Object>>();
		Scan scan = new Scan();
		if (startRow == null) {
			startRow = "";
		}
		if (stopRow == null) {
			stopRow = "";
		}
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setStopRow(Bytes.toBytes(stopRow));
		
		HTable hTable = (HTable) getHTable(tableName);
		if(hTable == null)
		{
			return recordsList;
		}
		try
		{
			ResultScanner rs = hTable.getScanner(scan);
			for (Result oneRow : rs)
			{
//				startRow = new String(oneRow.getRow());
				Map<String, Object> map = new HashMap<String, Object>();
				String row = new String(oneRow.getRow());
				// 把 一行记录的所有的列名-列值都取出来 ，放到map中 分别对应key-value 
				for (Cell cell : oneRow.listCells())
				{
//					row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
					map.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell
							.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//						System.out.println("列："
//								+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
//								+ "====值:" + Bytes.toString(cell
//										.getValueArray(), cell.getValueOffset(), cell.getValueLength()));			
				}
				map.put("row", row);
				recordsList.add(map);
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
		try
		{
			hTable.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return recordsList;
	}

	/**
	 * 获取hbase表中 指定页 的记录 <p>
	 * 本函数使用PageFilter，每次执行hTable.getScanner()只会取出pageSize+1条记录（相当于一页记录）。
   * 所以需要一页一页的将记录取出，直到取到指定页的记录，然后返回该页的记录。
	 * @param tableName 表空间+":"+表名
	 * @param pageNum 指定页数
	 * @param pageSize 每页大小
	 * @param startRow 启始行
	 * @param stopRow 终止行
	 * @return
	 */
	public List<Map<String, Object>> getPageRecordsWithRange(String tableName,int pageNum, int pageSize,String startRow,String stopRow)
	{
		Filter pageFilter = new PageFilter(pageSize + 1);
		List<Map<String, Object>> recordsList = new ArrayList<Map<String, Object>>();
		Scan scan = new Scan();
		scan.setFilter(pageFilter);
		if (startRow == null) {
			startRow = "";
		}
		if (stopRow == null) {
			stopRow = "";
		}
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setStopRow(Bytes.toBytes(stopRow));
		
		HTable hTable = (HTable) getHTable(tableName);
		if(hTable == null)
		{
			return recordsList;
		}
		// pageNum = 3 则需要扫描3页的数据
		for (int i = 0; i < pageNum; i++)
		{
			recordsList.clear();
			try
			{
				ResultScanner rs = hTable.getScanner(scan);
				int count = 0;
				for (Result oneRow : rs)
				{
					count++;
					if (count == pageSize + 1)
					{
						startRow = new String(oneRow.getRow());
						scan.setStartRow(startRow.getBytes());
//						System.out.println("startRow" + startRow);
						break;
					}
					startRow = new String(oneRow.getRow());
//					System.out.println("==================================="+startRow);
					
					Map<String, Object> map = new HashMap<String, Object>();
					// 把 一行记录的所有的列名-列值都取出来 ，放到map中 分别对应key-value 
					for (Cell cell : oneRow.listCells())
					{
						map.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell
								.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//						System.out.println("列："
//								+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
//								+ "====值:" + Bytes.toString(cell
//										.getValueArray(), cell.getValueOffset(), cell.getValueLength()));			

					}
//					System.out.println(count);
					map.put("row", startRow);
					recordsList.add(map);
				}
				
				if (count < pageSize)
				{
					break;
				}

			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		try
		{
			hTable.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return recordsList;
	}
	
	
	/**
	 * 获取表中 指定页 的记录 
	 * @param tableName 表空间+":"+表名
	 * @param pageNum 指定页数
	 * @param pageSize 每页大小
	 * @return
	 */
	public List<Map<String, Object>> getPageRecords(String tableName,int pageNum, int pageSize)
	{
		
		return getPageRecordsWithRange(tableName,pageNum, pageSize,null,null);
	}
	
	
	/**
	 * 获取hbase表的总页数
	 * @param tableName 表空间+":"+表名
	 * @param pageSize
	 * @return
	 */
	public int getTotalPage(String tableName, int pageSize) {
		
		if(!StringUtils.hasText(tableName))
		{
			return 0;
		}
		int total = 0;
		try {
			HTable hTable = (HTable) getHTable(tableName);
			Scan scan = new Scan();
			AggregationClient aggregation = new AggregationClient(configuration);
			Long count = aggregation.rowCount(hTable,new LongColumnInterpreter(), scan);
			total = count.intValue();
//			System.out.println(total);
			hTable.close();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
		}
		return (total % pageSize == 0) ? total / pageSize
				: (total / pageSize) + 1;
	}
	
	/**
	 * 查询满足条件的记录的总页数
	 * @param tableName 表空间+":"+表名
	 * @param pageSize
	 * @param scan 查询范围
	 * @return
	 */
	public int getTotalPageWithRange(String tableName, int pageSize,Scan scan) {
		
		if(!StringUtils.hasText(tableName))
		{
			return 0;
		}
		int total = 0;
		try {
			HTable hTable = (HTable) getHTable(tableName);
			AggregationClient aggregation = new AggregationClient(configuration);
			Long count = aggregation.rowCount(hTable,new LongColumnInterpreter(), scan);
			total = count.intValue();
//			System.out.println("================================="+total);
			hTable.close();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
		}
		return (total % pageSize == 0) ? total / pageSize
				: (total / pageSize) + 1;
	}
	
	/**
	 * 获取满足条件的表的总记录数
	 * @param tableName 表空间+":"+表名
	 * @param pageSize
	 * @return
	 */
	public int getTotalRecordNumWithRange(String tableName,Scan scan) {
		
		if(!StringUtils.hasText(tableName))
		{
			return 0;
		}
		
		int total = 0;
		try {
			HTable hTable = (HTable) getHTable(tableName);
			AggregationClient aggregation = new AggregationClient(configuration);
			Long count = aggregation.rowCount(hTable,
					new LongColumnInterpreter(), scan);
			total = count.intValue();
			hTable.close();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
		}
		return total;
	}
	/**
	 * 获取表的总记录数
	 * @param tableName 表空间+":"+表名
	 * @param pageSize
	 * @return
	 */
	public int getTotalRecordNum(String tableName) {
		
		if(!StringUtils.hasText(tableName))
		{
			return 0;
		}
		
		int total = 0;
		try {
			HTable hTable = (HTable) getHTable(tableName);
			Scan scan = new Scan();
			AggregationClient aggregation = new AggregationClient(configuration);
			Long count = aggregation.rowCount(hTable,
					new LongColumnInterpreter(), scan);
			total = count.intValue();
			hTable.close();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
		}
		return total;
	}
	
	/**
	 * 获取到指定表的连接，用完需要关闭
	 * @param tableName
	 * @return
	 */
	private HTableInterface getHTable(String tableName) {
		if (!StringUtils.hasText(tableName)) {
			return null;
		}
		HTableInterface hTable = null;
		try {
			hTable = conn.getTable(Bytes.toBytes(tableName));
		} catch (IOException e) {
			return null;
		}
		return hTable;
	}
	
	/**
	 * 本对象被回收时，关闭到大数据的连接池
	 */
	@Override
	public void finalize()
	{//perfect
		if(conn != null)
		{
			try
			{
				conn.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
//	/**
//	 * 使用程序的方式给hbase中的指定表加上协处理器
//	 * @param tableName
//	 */
//	private void enableAggregation(String tableName) {
//		String coprocessorName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
//		try {
//			HBaseAdmin admin = new HBaseAdmin(configuration);
//			HTableDescriptor htd = admin.getTableDescriptor(Bytes
//					.toBytes(tableName));
//			List<String> coprocessors = htd.getCoprocessors();
//			if (coprocessors != null && coprocessors.size() > 0) {
//				return;
//			} else {
//				admin.disableTable(tableName);
//				htd.addCoprocessor(coprocessorName);
//				admin.modifyTable(tableName, htd);
//				admin.enableTable(tableName);
//			}
//		} catch (TableNotFoundException e) {
//			// TODO Auto-generated catch block
//		} catch (MasterNotRunningException e) {
//			// TODO Auto-generated catch block
//		} catch (ZooKeeperConnectionException e) {
//			// TODO Auto-generated catch block
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//		}
//	}
}
