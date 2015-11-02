package geospatial1.operation1;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ClosestPair {
	public static void main(String[] args) {
		String inputFile = args[0];
		SparkConf conf = new SparkConf().setAppName("ClosestFurthestOperation");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputData = sc.textFile(inputFile).cache();
		JavaRDD<String> pointsData = inputData
				.filter(new Function<String, Boolean>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Boolean call(String s) {
						return !s.contains("x");
					}
				});

		JavaRDD<Point> points = pointsData.map(new Function<String, Point>() {
			public Point call(String row) {
				String[] xy = row.split(",");
				return new Point(Double.parseDouble(xy[0]), Double
						.parseDouble(xy[1]));
			}
		});
		final List<Point> listPoints = points.collect();
		sc.broadcast(listPoints);
		JavaPairRDD<PointPair, Double> pairs = points
				.mapToPair(new PairFunction<Point, PointPair, Double>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<PointPair, Double> call(Point point) {
						Tuple2<PointPair, Double> minTuple = null;
						double minDist = Double.POSITIVE_INFINITY;
						for (Point hullPoint : listPoints) {
							PointPair pointPair = new PointPair(point,
									hullPoint);
							double dist = pointPair.getDistance();

							if (dist != 0.0 && dist < minDist) {
								minDist = dist;
								minTuple = new Tuple2<PointPair, Double>(
										pointPair, dist);
							}
						}
						return minTuple;
					}
				});

		JavaPairRDD<PointPair, Double> finalPoints = pairs.sortByKey(true);
		Tuple2<PointPair, Double> input = finalPoints.first();
		System.out
				.println("############ Answer Final PointPair are Point1: X = "
						+ input._1().getP1().getX() + " and Y = "
						+ input._1().getP1().getY());
		System.out.println("############ Final PointPair are Point2: X = "
				+ input._1().getP2().getX() + " and Y = "
				+ input._1().getP2().getY());
		System.out.println("############ Final Distance for P1 and P2 = "
				+ input._2());
		// System.out.println("Lines with a: " + numAs + ", lines with b: " +
		// numBs);
	}

}
