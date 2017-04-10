package com.alibaba.datax.core.transport.channel.memory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存Channel的具体实现，底层其实是一个ArrayBlockingQueue
 *
 */
public class MemoryChannel extends Channel {

	private static final Logger LOG = LoggerFactory.getLogger(MemoryChannel.class);

	private AtomicLong checkpoint = new AtomicLong(-1);

	private CheckpointComparator checkpointComparator=null;

	private int bufferSize = 0;

	private AtomicInteger memoryBytes = new AtomicInteger(0);

	private ArrayBlockingQueue<Record> queue = null;

	private ReentrantLock lock;

	private Condition notInsufficient, notEmpty;

	public MemoryChannel(final Configuration configuration) {
		super(configuration);
		this.queue = new ArrayBlockingQueue<Record>(this.getCapacity());
		this.bufferSize = configuration.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);

		lock = new ReentrantLock();
		notInsufficient = lock.newCondition();
		notEmpty = lock.newCondition();
	}

	@Override
	public void close() {
		super.close();
		try {
			this.queue.put(TerminateRecord.get());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void clear(){
		this.queue.clear();
	}

	@Override
	protected void doPush(Record r) {
		try {
			long startTime = System.nanoTime();
			this.queue.put(r);
			waitWriterTime += System.nanoTime() - startTime;
            memoryBytes.addAndGet(r.getMemorySize());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doPushAll(Collection<Record> rs) {
		try {
			long startTime = System.nanoTime();
			lock.lockInterruptibly();
			int bytes = getRecordBytes(rs);
			while (memoryBytes.get() + bytes > this.byteCapacity || rs.size() > this.queue.remainingCapacity()) {
				notInsufficient.await(200L, TimeUnit.MILLISECONDS);
            }
			this.queue.addAll(rs);
			waitWriterTime += System.nanoTime() - startTime;
			memoryBytes.addAndGet(bytes);
			notEmpty.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	protected Record doPull() {
		try {
			long startTime = System.nanoTime();
			Record r = this.queue.take();
			waitReaderTime += System.nanoTime() - startTime;
			
			updateCheckpoint(Arrays.asList(r));	
			memoryBytes.addAndGet(-r.getMemorySize());
			return r;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected void doPullAll(Collection<Record> rs) {
		assert rs != null;
		rs.clear();
		try {
			long startTime = System.nanoTime();
			lock.lockInterruptibly();
			while (this.queue.drainTo(rs, bufferSize) <= 0) {
				notEmpty.await(200L, TimeUnit.MILLISECONDS);
			}
			waitReaderTime += System.nanoTime() - startTime;
			int bytes = getRecordBytes(rs);
			
			updateCheckpoint(rs);
			memoryBytes.addAndGet(-bytes);
			notInsufficient.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	private void updateCheckpoint(Collection<Record> rs) {
		long currCheckpoint=checkpoint.get();

		LOG.info("MemoryChannel configuration="+configuration.toJSON());

		String syncColumn=configuration.getString(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER_INCREMENTALSYNCCOLUMN);
		List<String> columns= configuration.getList(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER_COLUMNLIST,String.class);
		int index=-1;
		for(int i=0;i<columns.size();i++){
                    if(columns.get(i).equals(syncColumn)) {
                        index=i;
                        break;
                    }
                }
		if(index==-1)
                    throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    String.format("最可能的原因是增量同步字段[%s]没有包含在[%s]", syncColumn,columns));
		if(((Record)rs.toArray()[0]) instanceof TerminateRecord){
			LOG.info("Record is TerminateRecord, skipped.");
			return;
		}
		checkpointComparator=new CheckpointComparator(index);
		long newCheckpoint= Collections.max(rs,checkpointComparator).getColumn(index).asLong();
		if(newCheckpoint-currCheckpoint>0) {
			checkpoint.compareAndSet(currCheckpoint, newCheckpoint);
		}
		Column.Type columnType=((Record)rs.toArray()[0]).getColumn(index).getType();
		Map map=configuration.getMap(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER_CHECKPOINT);
		map.put(configuration.getString(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER_TABLE)+"."+syncColumn,format(columnType,checkpoint.get()));
		configuration.set(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER_CHECKPOINT,map);
		LOG.info("configuration="+configuration.toString());
	}

	private String format(Column.Type columnType, long value) {
		switch (columnType) {
			case DATE:
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				return format.format(new Date(value));
			case INT:
			case LONG:
			case DOUBLE:
				return String.valueOf(value);
			default:
				LOG.warn("not a valid value="+value );
				return String.valueOf(value);
		}
	}

	private class CheckpointComparator implements Comparator<Record> {
		int columnIndex=-1;
		public  CheckpointComparator(int columnIndex){
			this.columnIndex=columnIndex;
		}
		@Override
		public int compare(Record r1, Record r2) {
			// 正数代表第一个数大于第二个数
			if(r1 instanceof TerminateRecord)
				return -1;
			if(r2 instanceof TerminateRecord)
				return 1;
			return (r1.getColumn(columnIndex).asLong() < r2.getColumn(columnIndex).asLong() ? -1 : (r1.getColumn(columnIndex).asLong() == r2.getColumn(columnIndex).asLong() ? 0 : 1));
		}
	}

	private int getRecordBytes(Collection<Record> rs){
		int bytes = 0;
		for(Record r : rs){
			bytes += r.getMemorySize();
		}
		return bytes;
	}

	@Override
	public int size() {
		return this.queue.size();
	}

	@Override
	public boolean isEmpty() {
		return this.queue.isEmpty();
	}

}
