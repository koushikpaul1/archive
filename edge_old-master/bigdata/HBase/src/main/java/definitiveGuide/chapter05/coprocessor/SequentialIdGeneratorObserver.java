package definitiveGuide.chapter05.coprocessor;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

// cc SequentialIdGeneratorObserver Adds a coprocessor local ID into the operation
public class SequentialIdGeneratorObserver extends BaseRegionObserver {
  public static final Log LOG = LogFactory.getLog(HRegion.class);

  // vv SequentialIdGeneratorObserver
  private static String KEY_ID = "X-ID-GEN";
  private byte[] family;
  private byte[] qualifier;
  private String regionName;

  private Random rnd = new Random();
  private int delay;

   
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      Configuration conf = env.getConfiguration(); // co SequentialIdGeneratorObserver-1-Conf Get environment and configuration instances.
      this.regionName = env.getRegionInfo().getEncodedName();
      String family = conf.get("com.larsgeorge.copro.seqidgen.family", "cf1");
      this.family = Bytes.toBytes(family);
      String qualifier = conf.get("com.larsgeorge.copro.seqidgen.qualifier", // co SequentialIdGeneratorObserver-2-Settings Retrieve the settings passed into the configuration.
        "GENID");
      this.qualifier = Bytes.toBytes(qualifier);
      int startId = conf.getInt("com.larsgeorge.copro.seqidgen.startId", 1);
      this.delay = conf.getInt("com.larsgeorge.copro.seqidgen.delay", 100);
      env.getSharedData().putIfAbsent(KEY_ID, new AtomicInteger(startId)); // co SequentialIdGeneratorObserver-3-Gen Set up generator if this has not been done yet on this region server.
    } else {
      LOG.warn("Received wrong context.");
    }
  }

   
  public void stop(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      AtomicInteger id = (AtomicInteger) env.getSharedData().get(KEY_ID);
      LOG.info("Final ID issued: " + regionName + "-" + id.get()); // co SequentialIdGeneratorObserver-4-Log Log the final number generated by this coprocessor.
    } else {
      LOG.warn("Received wrong context.");
    }
  }

   
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
    WALEdit edit, Durability durability) throws IOException {
    RegionCoprocessorEnvironment env = e.getEnvironment();
    AtomicInteger id = (AtomicInteger) env.getSharedData().get(KEY_ID);
    put.addColumn(family, qualifier, Bytes.toBytes(regionName + "-" + // co SequentialIdGeneratorObserver-5-SetId Set the shared ID for this instance of put.
      id.incrementAndGet()));

    try {
      Thread.sleep(rnd.nextInt(delay)); // co SequentialIdGeneratorObserver-6-Sleep Sleep for 0 to "delay" milliseconds.
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
  }
  // ^^ SequentialIdGeneratorObserver
}
