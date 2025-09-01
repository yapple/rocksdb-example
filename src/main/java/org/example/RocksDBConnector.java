package org.example;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.RestExpress;
import org.terarkdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
*/

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 
 The given code block contains a simple counter using rocksdb's merge operator
 
 the Counters Fails for the following Cases
 
 $ ab -r -k -n 10000 -c 1000 http://localhost:9009/increment

 $ curl http://localhost:9009/get                                                                                                                                                           

 Current RocksDB Count is 137   (Expected value is 10000)
 Current AtomicLong Count is 10000

 $ curl http://localhost:9009/reset

 Current RocksDB Count is 0
 Current AtomicLong Count is 0

 $ ab -r -k -n 10000 -c 1000 http://localhost:9009/batchIncrement

 $ curl http://localhost:9009/get

 Current RocksDB Count is 16   (Expected value is 10000)
 Current AtomicLong Count is 10000
 
 */
public class RocksDBConnector {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBConnector.class);
    private  static  RocksDB db;
    private  RocksDBConnector(){}
    private final static RocksDBConnector instance = new RocksDBConnector();
    public static void main(String[] args) {
        try {
            initializeRocksDb();
            db_load();
        } catch (IOException e) {
            System.out.print(e);
            LOG.info(e.getMessage());
        } catch (RocksDBException e) {
            System.out.print(e);
            LOG.info(e.getMessage());
        }
    }
    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
        int number=random.nextInt(62);
        sb.append(str.charAt(number));
        }
        return sb.toString();
    }
    private static void db_load()throws RocksDBException{

      for (int i = 0; i < 1000; i += 2) {
        String key = getRandomString(8);
        String value = getRandomString(i);
        byte[] valueBytes = value.getBytes();
        byte[] ts1 = encodeFixed64(i);
        db.put(key.getBytes(), ts1, valueBytes);
        byte[] ts2 = encodeFixed64(i + 1);
        ReadOptions ro = new ReadOptions();
        ro.setTimeStamp(new Slice(ts2));
        byte[] vBytes = new byte[1024];
        int r = db.get(ro, key.getBytes(), vBytes);
        if (r > 0) {
          byte[] vBytesSub = Arrays.copyOfRange(vBytes, 0, r);
          if (Arrays.equals(vBytesSub, valueBytes)) {
            System.out.println("v 和 value 相等");
          } else {
            System.out.println(vBytesSub);
            System.out.println("v 和 value 不相等");
          }
        } else {
            System.out.println("v 不存在");
        }
        System.out.println("finished load" + String.valueOf(i));
      }
        System.out.println("finished load");
        db.compactRange();
    }

    // 判断系统原生字节序是否为小端
    private static final boolean IS_LITTLE_ENDIAN =
        ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    /**
     * 将 long 类型编码为固定 8 字节的字节数组
     * @param value 要编码的 long 值
     * @return 编码后的 8 字节数组
     */
    public static byte[] encodeFixed64(long value) {
      byte[] result = new byte[8];
      ByteBuffer buffer = ByteBuffer.wrap(result);

      // 根据系统原生字节序编码
      if (IS_LITTLE_ENDIAN) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
      } else {
        buffer.order(ByteOrder.BIG_ENDIAN);
      }

      buffer.putLong(value);
      return result;
    }

    private  static void initializeRocksDb() throws RocksDBException,UnsupportedEncodingException{
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            options.setMergeOperatorName("uint64add");
            options.setMaxBackgroundFlushes(1);
            options.setWriteBufferSize(50L);
            options.setBlobSize(256);
            options.setCreateMissingColumnFamilies(true);
            options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_U64TS);
            // CompactionOptionsFIFO fifo = new CompactionOptionsFIFO();
            // options.setCompactionOptionsFIFO(fifo);
            // options.setCompactionStyle(CompactionStyle.FIFO);
            // options.setStatsDumpPeriodSec(10);

            if (db == null) {
                db = RocksDB.open( options, "/tmp/testdata");
            }
    }
}
