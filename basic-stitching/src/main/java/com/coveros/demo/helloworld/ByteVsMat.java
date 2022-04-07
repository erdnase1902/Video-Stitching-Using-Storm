package com.coveros.demo.helloworld;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_stitching.Stitcher;
import org.opencv.core.MatOfByte;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;
import static org.bytedeco.opencv.global.opencv_stitching.createStitcher;
public class ByteVsMat {
    public static void main(final String[] args) throws IOException {
        byte[] img_arr = null;
        img_arr = Files.readAllBytes(Paths.get("/vagrant/basic-stitching/sample-left.jpg"));
        org.bytedeco.opencv.opencv_core.Mat img_mat_temp = new org.bytedeco.opencv.opencv_core.Mat(img_arr, false);
        org.bytedeco.opencv.opencv_core.Mat img_mat = imdecode(img_mat_temp, -1);
        imwrite("result.jpg", img_mat);
    }
}
