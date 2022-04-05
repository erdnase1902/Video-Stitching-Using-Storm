package com.coveros.demo.helloworld;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_stitching.Stitcher;
import org.bytedeco.opencv.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;
import static org.bytedeco.opencv.global.opencv_stitching.WAVE_CORRECT_HORIZ;
import static org.bytedeco.opencv.global.opencv_stitching.createStitcher;

public class HelloWorld {
  static boolean try_use_gpu = false;


  public static void main(final String[] args) {
    final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("h:mm:ss a 'on' MMMM d, yyyy'.'");
    final LocalDateTime now = LocalDateTime.now();
    cool_stitch("a", "b");
    System.out.println("Hello, World! The current time is " + dtf.format(now));
  }
  public static void smooth(String filename) {
    Mat image = imread(filename);
    if (image != null) {
      GaussianBlur(image, image, new Size(3, 3), 0);
      imwrite(filename, image);
    }
  }

  public static void cool_stitch(String filename1, String filename2) {
    Mat left_img = imread(/*filename1*/"sample-left.jpg");
    Mat right_img = imread(/*filename2*/"sample-right.jpg");
//    MatVector imgs = new MatVector(left_img, right_img);

    MatVector imgs = new MatVector();
    imgs.resize(1);
    imgs.put(0, left_img);
    imgs.resize(2);
    imgs.put(1, right_img);

    Stitcher stitcher = createStitcher(false);//new Stitcher();
    Mat pano = new Mat();

    int status = stitcher.stitch(imgs, pano);

    if (status != Stitcher.OK) {
      System.out.println("Can't stitch images, error code = " + status);
      System.exit(-1);
    }

    imwrite("result.jpg", pano);





  }
}
