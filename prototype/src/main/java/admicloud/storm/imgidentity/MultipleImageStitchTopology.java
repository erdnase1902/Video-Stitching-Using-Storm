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
import org.bytedeco.javacpp.BytePointer;
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
import static org.opencv.core.CvType.CV_8UC;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Arrays;

public class MultipleImageStitchTopology {




    private static Logger LOG = LoggerFactory.getLogger(IdentityImageTopology.class);

    public static class RandomImageSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        Random _rand;

        File folder = new File("/home/vagrant/sample-video/frames_split");
        File[] listOfFiles = folder.listFiles();
        File[] leftFiles;
        File[] rightFiles;

        int fileCounter = 0;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
            leftFiles = Arrays.stream(listOfFiles)
                    .filter(file -> file.isFile() && file.getName().contains("l"))
                    .toArray(File[]::new);
            rightFiles = Arrays.stream(listOfFiles)
                    .filter(file -> file.isFile() && file.getName().contains("r"))
                    .toArray(File[]::new);
            Arrays.sort(leftFiles, Comparator.comparingInt(f -> Integer.parseInt(f.getName().substring(3, f.getName().length() - 5)))
            );

            Arrays.sort(rightFiles, Comparator.comparingInt(f -> Integer.parseInt(f.getName().substring(3, f.getName().length() - 5)))
            );
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            byte[] randomImg = null;
            byte[] randomImg2 = null;
            try {
                randomImg = Files.readAllBytes(Paths.get(leftFiles[fileCounter].getAbsolutePath()));
                randomImg2 = Files.readAllBytes(Paths.get(rightFiles[fileCounter].getAbsolutePath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (fileCounter < leftFiles.length - 1) {
                fileCounter += 1;
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
        Stitcher stitcher;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);
            stitcher = createStitcher(false);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result", "rows", "cols", "channels"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            byte[] img = tuple.getBinaryByField("img");
            byte[] img2 = tuple.getBinaryByField("img2");


            // Fix below code block by referring to ByteVsMat
            org.bytedeco.opencv.opencv_core.Mat left_img = imdecode(new org.bytedeco.opencv.opencv_core.Mat(img, false), -1);
            org.bytedeco.opencv.opencv_core.Mat right_img = imdecode(new org.bytedeco.opencv.opencv_core.Mat(img2, false), -1);
            MatVector imgs = new MatVector();
            imgs.resize(1);
            imgs.put(0, left_img);
            imgs.resize(2);
            imgs.put(1, right_img);


            org.bytedeco.opencv.opencv_core.Mat pano = new org.bytedeco.opencv.opencv_core.Mat();

            int status = stitcher.stitch(imgs, pano);

            if (status != Stitcher.OK) {
                System.out.println("Can't stitch images, error code = " + status);

            }
            else {
                byte[] pano_bytearr = new byte[pano.channels()*pano.cols()*pano.rows()];
                pano.data().get(pano_bytearr);
                basicOutputCollector.emit(new Values(pano_bytearr, pano.rows(), pano.cols(), pano.channels()));
            }

        }
    }

    public static class SaveImg extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        Integer count = 0;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            byte[] img = tuple.getBinaryByField("result");
            int rows = tuple.getIntegerByField("rows");
            int cols = tuple.getIntegerByField("cols");
            int channels = tuple.getIntegerByField("channels");



//            org.bytedeco.opencv.opencv_core.Mat img_mat = new org.bytedeco.opencv.opencv_core.Mat(img, false);
            org.bytedeco.opencv.opencv_core.Mat img_mat = new org.bytedeco.opencv.opencv_core.Mat(rows, cols, CV_8UC(channels), new BytePointer(img));

            String filename = count.toString() + ".jpg";
            count = count + 1;

            imwrite("/home/vagrant/out_imgs/" + filename, img_mat);

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
            conf.setNumWorkers(1);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("multi-image", conf, builder.createTopology());

//            Thread.sleep(10000000);

//            cluster.shutdown();
        }
    }
}
