package org.example;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.RestExpress;
import org.rocksdb.*;
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

        for(int i =0 ;i< 1000;i++){
            String key = getRandomString(8);
            String value = getRandomString(i);
            for(int j = 0;j < 1000;j++){ 
                db.put(key.getBytes(),value.getBytes());
            }
            System.out.println("finished load" + String.valueOf(i));
        }
        System.out.println("finished load");
        db.compactRange();
    }
        
    private  static void initializeRocksDb() throws RocksDBException,UnsupportedEncodingException{
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            options.setMergeOperatorName("uint64add");
            options.setMaxBackgroundFlushes(1);
            options.setWriteBufferSize(50L);
            options.setBlobSize(256);
            options.setCreateMissingColumnFamilies(true);
            if (db == null) {
                db = RocksDB.open( options, "/tmp/testdata");
            }
    }

}