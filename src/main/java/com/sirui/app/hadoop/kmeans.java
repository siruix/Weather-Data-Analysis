package com.sirui.app.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class kmeans {
    public static void main(String args[]) throws Exception {

        int k = 2;

        File file = new File("input");
        if (!file.exists()) {
            file.mkdir();
        }
        file = new File("output");
        if (!file.exists()) {
            file.mkdir();
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String path = new String("input/prcp_namedvector_month.out");
        File infile = new File(path);
        Path path_clusters = new Path("clusters/part-randomSeed");
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path_clusters, Text.class, Kluster.class);
        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        for (String line : FileUtils.readLines(infile))
        {
        		key.set(ctr++);
        		value.set(line);
        		if (ctr < 150)
        			System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);			
        		writer.append(key, value);
        }
 //       writePointsToFile(vectors, "clustering/testdata/points/file1", fs, conf);

        

        for (int i = 0; i < k; i++) {
            Vector vec = vectors.get(i);
            Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();

        KMeansDriver.run(conf,
                new Path("input"),
                new Path("clusters"),
                new Path("output"),
                0.001,
                10,
                true,
                0,
                true);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                new Path("output" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0"), conf);

        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        while (reader.next(key, value)) {
            System.out.println(value.toString() + " belongs to cluster " + key.toString());
        }
        reader.close();
    }

}