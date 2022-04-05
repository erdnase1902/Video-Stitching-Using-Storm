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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SingleImageStitchTopology {
    private static Logger LOG = LoggerFactory.getLogger(IdentityImageTopology.class);

    public static class RandomImageSpout extends BaseRichSpout {
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
            byte[] randomImg = null;
            byte[] randomImg2 = null;
            try {
                randomImg = Files.readAllBytes(Paths.get("/vagrant/prototype/sample-left.jpg"));
                randomImg2 = Files.readAllBytes(Paths.get("/vagrant/prototype/sample-right.jpg"));
            } catch (IOException e) {
                e.printStackTrace();
            }
//      _collector.emit(new Values(sentence));
            _collector.emit(new Values(randomImg, randomImg2));

        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("img", "img2"));
        }
    }

    public static class DoNothing extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("img", "img2"));
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
            byte[] img2 = tuple.getBinaryByField("img2");
            basicOutputCollector.emit(new Values(img, img2));
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
            byte[] img2 = tuple.getBinaryByField("img2");
            String filename = count.toString() + ".jpg";
            count = count + 1;

            try {
                FileUtils.writeByteArrayToFile(new File("/home/vagrant/out_imgs/" + filename), img2);
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
            cluster.submitTopology("single-image", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
