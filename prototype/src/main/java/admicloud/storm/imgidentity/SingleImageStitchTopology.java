package admicloud.storm.imgidentity;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.shade.org.apache.commons.lang.ObjectUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.Option;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_stitching.Stitcher;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;
import static org.bytedeco.opencv.global.opencv_stitching.createStitcher;



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

    public static class StitchBolt extends BaseBasicBolt {
        Stitcher stitcher = createStitcher(false);

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            byte[] img = tuple.getBinaryByField("img");
            byte[] img2 = tuple.getBinaryByField("img2");


            // Fix below code block by referring to ByteVsMat
            org.bytedeco.opencv.opencv_core.Mat left_img = new org.bytedeco.opencv.opencv_core.Mat(img);
            org.bytedeco.opencv.opencv_core.Mat right_img = new org.bytedeco.opencv.opencv_core.Mat(img2);
            MatVector imgs = new MatVector();
            imgs.resize(1);
            imgs.put(0, left_img);
            imgs.resize(2);
            imgs.put(1, right_img);


            org.bytedeco.opencv.opencv_core.Mat pano = new org.bytedeco.opencv.opencv_core.Mat();

            int status = stitcher.stitch(imgs, pano);

            if (status != Stitcher.OK) {
                System.out.println("Can't stitch images, error code = " + status);
                System.exit(-1);
            }

            imwrite("result.jpg", pano);


            basicOutputCollector.emit(new Values(img));
        }
    }

    public static class SaveImg extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        Integer count = 0;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
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

        builder.setBolt("split", new StitchBolt(), 1).shuffleGrouping("spout");
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
