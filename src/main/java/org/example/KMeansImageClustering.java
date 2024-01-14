package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class KMeansImageClustering {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: KMeansImageClustering <imagePath> <outputPath> <centersPath>");
            System.exit(1);
        }

        String imagePath = args[0];
        String outputPath = args[1];
        String centersPath = args[2];

        SparkConf conf = new SparkConf().setAppName("ImageKMeans");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the image data from HDFS
        JavaRDD<Vector> imageData = loadImageData(sc, imagePath);

        // Load initial cluster centers from centers.txt
        JavaRDD<Vector> initialCenters = loadInitialCenters(sc, centersPath);

        // Set the number of clusters (in this case, 3)
        int numClusters = 3;

        // Set the number of iterations
        int numIterations = 20;

        // Train the KMeans model
        KMeansModel kMeansModel = KMeans.train(imageData.rdd(), numClusters, numIterations, String.valueOf(initialCenters.rdd()));

        // Get the cluster centers
        Vector[] clusterCenters = kMeansModel.clusterCenters();

        // Save the cluster centers to a file
        saveClusterCenters(sc, clusterCenters, outputPath + "/clusterCenters");

        // Get the cluster assignments for each pixel
        JavaRDD<Integer> clusterAssignments = kMeansModel.predict(imageData);

        // Save the cluster assignments to a file
        clusterAssignments.saveAsTextFile(outputPath + "/clusterAssignments");

        sc.stop();
    }

    // Implement a method to load image data and convert it to feature vectors
    private static JavaRDD<Vector> loadImageData(JavaSparkContext sc, String imagePath) {
        try {
            byte[] imageBytes = FileUtils.readFileToByteArray(new File(imagePath));
            double[] pixels = new double[imageBytes.length];
            for (int i = 0; i < imageBytes.length; i++) {
                pixels[i] = (double) (imageBytes[i] & 0xff);
            }
            return sc.parallelize(Arrays.asList(Vectors.dense(pixels)));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Implement a method to load initial cluster centers from centers.txt
    private static JavaRDD<Vector> loadInitialCenters(JavaSparkContext sc, String centersPath) {
        try {
            List<String> lines = sc.textFile(centersPath).collect();
            return sc.parallelize(lines).map(line -> {
                String[] values = line.split(",");
                double[] center = Arrays.stream(values).mapToDouble(Double::parseDouble).toArray();
                return Vectors.dense(center);
            });
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Implement a method to save cluster centers to a file
    private static void saveClusterCenters(JavaSparkContext sc, Vector[] clusterCenters, String outputPath) {
        sc.parallelize(Arrays.asList(clusterCenters))
                .map(Vector::toString)
                .saveAsTextFile(outputPath);
    }
}
