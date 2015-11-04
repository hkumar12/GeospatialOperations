package geospatial1.spatialQuery;

import geospatial1.operation1.Point;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class SpatialRangeQuery {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SpatialRangeQuery Application");

		JavaSparkContext sc = new JavaSparkContext(conf);
		// Read the input csv file holding set of polygons in a rdd of string
		// objects
		JavaRDD<String> inputPoints = sc.textFile("hdfs://192.168.139.149:54310/harsh/spatialRangePointInput.csv");

		// Map the above rdd of strings to a rdd of points
		// This is done by splitting the rows of file by ‘,’ and then converting
		// them to individual
		// points.

		JavaRDD<Point> pointRDD = inputPoints.map(mapInputStringToPointRDD());

		// Repeat the above process but now for initializing the rdd for query
		// window
		JavaRDD<String> queryRect = sc.textFile("hdfs://192.168.139.149:54310/harsh/spatialRangeWindowInput.csv");
		// Map the query window to RDD object
		JavaRDD<Rectangle> queryRDD = queryRect.map(new Function<String, Rectangle>() {
			public Rectangle call(String inputString) {
				String[] points = inputString.split(",");
				System.out.println("----------------------------------------------------------------------");
				System.out.println(points[0]);
				System.out.println(points[1]);
				
				System.out.println("----------------------------------------------------------------------");
				Double leftMostUpperXCoord = Math.min(Double.parseDouble(points[0]), Double.parseDouble(points[2]));
				Double leftMostUpperYCoord = Math.max(Double.parseDouble(points[1]), Double.parseDouble(points[3]));

				Point upperLeftPoint = new Point(leftMostUpperXCoord, leftMostUpperYCoord);
				Double rightMostLowerXCoord = Math.max(Double.parseDouble(points[0]), Double.parseDouble(points[2]));
				Double rightMostLowerYCoord = Math.min(Double.parseDouble(points[1]), Double.parseDouble(points[3]));
				Point lowerRightPoint = new Point(rightMostLowerXCoord, rightMostLowerYCoord);
				return new Rectangle(0, upperLeftPoint, lowerRightPoint);
			}
		});

		// Broadcast the query window to each of the worker
		final Broadcast<Rectangle> queryWindow = sc.broadcast(queryRDD.first());
		// Filter the RDD for the input rectangles formed earlier based upon the
		// query window
		// by utilizing the isRectangleinsideQueryWindow() method as described
		// previously
		// while creating the Rectangle class.
		System.out.println(">>>>>>>>>>>>>>>>>>>>PRINTPOINT...................................................");
		System.out.println(pointRDD.collect());
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		JavaRDD<Point> rangeQueryRDD = pointRDD.filter(new Function<Point, Boolean>() {
			public Boolean call(Point point) throws Exception {
				System.out.println(">>>>>>>>>>>>>>INSIDE>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				// call the method of the rectangle class as described
				// above
				System.out.println("***************************");
				System.out.println(point.getX());
				System.out.println(point.getY());
				System.out.println("***************************");
				return point.isPointinsideQueryWindow(queryWindow.value());
			}
		});
		System.out.println(">>>>>>>>>>>>>>>>>>>>PRINTRANGE...................................................");
		System.out.println(rangeQueryRDD.collect());
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		// Save the result RDD object as a file on HDFS
		
		JavaRDD<String> result = rangeQueryRDD.map(new Function<Point, String>() {
			public String call(Point inputPoint) {
				return inputPoint.getPointID().toString();
			}
		});
		
		result.coalesce(1).saveAsTextFile("hdfs://192.168.139.149:54310/harsh/rqueryResult.csv");
		sc.close();
	}

	public static Function<String, Point> mapInputStringToPointRDD() {
		return new Function<String, Point>() {
			public Point call(String inputString) {
				String[] points = inputString.split(",");

				// Initialize the point by the above pair

				return new Point(Integer.parseInt(points[0]), Double.parseDouble(points[1]), Double
						.parseDouble(points[2]));

			}
		};
	}
}
