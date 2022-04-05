package admicloud.storm.imgidentity;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class IdentityImageTopology {
  private static Logger LOG = LoggerFactory.getLogger(IdentityImageTopology.class);

  public static class RandomImageSpout extends BaseRichSpout {
    private static byte[] getImageBytes(String imageUrl) throws IOException {
      /*
       * https://stackoverflow.com/questions/2295221/java-net-url-read-stream-to-byte
       * */
      URL url = new URL(imageUrl);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      InputStream is = null;
      try {
        is = url.openStream ();
        byte[] byteChunk = new byte[4096]; // Or whatever size you want to read in at a time.
        int n;

        while ( (n = is.read(byteChunk)) > 0 ) {
          baos.write(byteChunk, 0, n);
        }
      }
      catch (IOException e) {
        System.err.printf ("Failed while reading bytes from %s: %s", url.toExternalForm(), e.getMessage());
        e.printStackTrace ();
        // Perform any other exception handling that's appropriate.
      }
      finally {
        if (is != null) { is.close(); }
      }
      return baos.toByteArray();
    }
    SpoutOutputCollector _collector;
    Random _rand;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = new Random();
    }

    @Override
    public void nextTuple() {
      Utils.sleep(500);
      String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
          "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
      String sentence = sentences[_rand.nextInt(sentences.length)];
      byte[] randomImg = null;
      try {
        randomImg = getImageBytes("https://picsum.photos/200");
      } catch (IOException e) {
        e.printStackTrace();
      }
//      _collector.emit(new Values(sentence));
      _collector.emit(new Values(randomImg));

    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("img"));
    }
  }

  public static class DoNothing extends BaseBasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("img"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
      /*
      String sentence = tuple.getStringByField("sentence");
      String words[] = sentence.split(" ");
      for (String w : words) {
        basicOutputCollector.emit(new Values(w));
      }
      */
      byte[] img = tuple.getBinaryByField("img");
      basicOutputCollector.emit(new Values(img));
    }
  }

  public static class SaveImg extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    Integer count = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      /*
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      LOG.info("Count of word: " + word + " = " + count);
      collector.emit(new Values(word, count));
      * */
      byte[] img = tuple.getBinaryByField("img");
      String filename = count.toString() + ".jpg";
      count = count + 1;

      try {
        FileUtils.writeByteArrayToFile(new File("/home/vagrant/out_imgs/" + filename), img);
      } catch (IOException e) {
        e.printStackTrace();
      }
      collector.emit(new Values("word", count));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomImageSpout(), 1);

    builder.setBolt("split", new DoNothing(), 1).shuffleGrouping("spout");
    builder.setBolt("count", new SaveImg(), 1).shuffleGrouping("split");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(1);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("identity-image", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
