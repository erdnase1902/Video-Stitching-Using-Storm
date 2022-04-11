package com.coveros.demo.helloworld;

import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.MatVector;
import org.bytedeco.opencv.opencv_stitching.Stitcher;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

import static org.bytedeco.opencv.global.opencv_imgcodecs.imread;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imwrite;
import static org.bytedeco.opencv.global.opencv_stitching.createStitcher;

public class MultipleImageStitch {
    public static void main(String[] args) {
        File folder = new File("/home/vagrant/sample-video/frames_split");
        File[] listOfFiles = folder.listFiles();

        File[] leftFiles = Arrays.stream(listOfFiles)
                .filter(file -> file.isFile() && file.getName().contains("l"))
                .toArray(File[]::new);

        File[] rightFiles = Arrays.stream(listOfFiles)
                .filter(file -> file.isFile() && file.getName().contains("r"))
                .toArray(File[]::new);
        int fileNameCounter = 0;


        Arrays.sort(leftFiles, Comparator.comparingInt(f -> Integer.parseInt(f.getName().substring(3, f.getName().length() - 5)))
                );

        Arrays.sort(rightFiles, Comparator.comparingInt(f -> Integer.parseInt(f.getName().substring(3, f.getName().length() - 5)))
        );
        

        String[] seqNumsString = Arrays.stream(leftFiles)
                .map(file -> file.getName().substring(3, file.getName().length() - 5))
                .toArray(String[]::new);

        Stitcher stitcher = createStitcher(false);

        for (int idx = 0; idx < seqNumsString.length; idx++) {
            Mat pano = new Mat();
            MatVector imgs = new MatVector();

            Mat leftImg = imread(leftFiles[idx].getAbsolutePath());
            Mat rightImg = imread(rightFiles[idx].getAbsolutePath());

            imgs.push_back(leftImg);
            imgs.push_back(rightImg);

            int status = stitcher.stitch(imgs, pano);

            if (status != Stitcher.OK) {
                System.err.println("Can't stitch images, error code = " + status);
                continue;
            }

            imwrite(Paths.get("/home/vagrant/out_imgs/", fileNameCounter + ".jpg").toString(), pano);
            fileNameCounter++;

        }

    }
}
