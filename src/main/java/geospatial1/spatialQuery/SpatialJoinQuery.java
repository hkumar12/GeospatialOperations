package geospatial1.spatialQuery;

import geospatial1.operation1.Point;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SpatialJoinQuery {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SpatialJoinQuery Application");

		JavaSparkContext sc = new JavaSparkContext(conf);
		// Read the input csv file holding set of polygons in a rdd of string
		// objects
		JavaRDD<String> firstInputPoints = sc.textFile("hdfs://192.168.139.149:54310/harsh/spatialJoinFirstInput.csv");

		// Map the above rdd of strings to a rdd of rectangles for the first
		// input

		// Repeat the above process but now for initializing the rdd for query
		// window
		JavaRDD<String> secondInputPoints = sc
				.textFile("hdfs://192.168.139.149:54310/harsh/spatialJoinSecondInput.csv");

		JavaRDD<Tuple2<Integer, ArrayList<Integer>>> joinQueryRDD = new JavaRDD<Tuple2<Integer, ArrayList<Integer>>>(
				null, null);

		if (args[0].equalsIgnoreCase("rectangle")) {
			
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			System.out.println("inside>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

			final JavaRDD<Rectangle> firstInputRDD = firstInputPoints.map(mapInputStringToRectRDD());
			System.out.println(firstInputRDD.collect());

			// Map the query window to RDD object
			final JavaRDD<Rectangle> secondInputRDD = secondInputPoints.map(mapInputStringToRectRDD());

			// broadcast the second set of rectangles to each of the workers
			final Broadcast<List<Rectangle>> firstInput = sc.broadcast(firstInputRDD.collect());
			// map the id of first input to the multiple id’s of the second
			// input if
			// they contain the
			// first rectangle.
			joinQueryRDD = secondInputRDD.map(new Function<Rectangle, Tuple2<Integer, ArrayList<Integer>>>() {
				public Tuple2<Integer, ArrayList<Integer>> call(Rectangle rectangle) throws Exception {
					// Get the list of rectangles from the second RDD input.
					List<Rectangle> firstInputCollection = firstInput.value();
					ArrayList<Integer> secondInputIds = new ArrayList<Integer>();
					// Iterate the second input and check for the second set
					// of rectangle id’s
					// that hold the rectangle from first set obtained from
					// the mapped RDD
					for (Rectangle firstRects : firstInputCollection) {
						if (rectangle.isRectangleinsideQueryWindow(firstRects)) {
							secondInputIds.add(firstRects.getRectangleId());
						}
					}
					// Create a new tuple of the mapped values and return
					// back the mapped
					// transformation.
					Tuple2<Integer, ArrayList<Integer>> resultList = new Tuple2<Integer, ArrayList<Integer>>(rectangle
							.getRectangleId(), secondInputIds);
					return resultList;
				}
			});

		} else if (args[0].equalsIgnoreCase("point")) {

			final JavaRDD<Point> firstInputRDD = firstInputPoints.map(SpatialRangeQuery.mapInputStringToPointRDD());
			
			// broadcast the second set of rectangles to each of the workers
			final Broadcast<List<Point>> firstInput = sc.broadcast(firstInputRDD.collect());
			
			// Map the query window to RDD object
			final JavaRDD<Rectangle> secondInputRDD = secondInputPoints.map(mapInputStringToRectRDD());

			joinQueryRDD = secondInputRDD.map(new Function<Rectangle, Tuple2<Integer, ArrayList<Integer>>>() {
				public Tuple2<Integer, ArrayList<Integer>> call(Rectangle rectangle) throws Exception {
					// Get the list of rectangles from the second RDD input.
					List<Point> firstInputCollection = firstInput.getValue();
					ArrayList<Integer> secondInputIds = new ArrayList<Integer>();
					// Iterate the second input and check for the second set
					// of rectangle id’s
					// that hold the rectangle from first set obtained from
					// the mapped RDD
					for (Point point : firstInputCollection) {
						if (point.isPointinsideQueryWindow(rectangle)) {
							secondInputIds.add(point.getPointID());
						}
					}
					// Create a new tuple of the mapped values and return
					// back the mapped
					// transformation.
					Tuple2<Integer, ArrayList<Integer>> resultList = new Tuple2<Integer, ArrayList<Integer>>(rectangle
							.getRectangleId(), secondInputIds);
					return resultList;
				}
			});

		}
		
		JavaRDD<String> result = joinQueryRDD.map(new Function<Tuple2<Integer, ArrayList<Integer>>, String>() {
			public String call(Tuple2<Integer, ArrayList<Integer>> inputPoint) {

				Integer containingRect = inputPoint._1();
				ArrayList<Integer> containedRects = inputPoint._2();

				StringBuffer intermediateBuffer = new StringBuffer();
				
				intermediateBuffer.append(containingRect);

				for (Integer rects : containedRects) {
					intermediateBuffer.append(", " + rects);
				}

				return intermediateBuffer.toString();
			}
		});

		result.coalesce(1).saveAsTextFile("hdfs://192.168.139.149:54310/harsh/jQueryResult.csv");

		sc.close();
	}

	private static Function<String, Rectangle> mapInputStringToRectRDD() {
		return new Function<String, Rectangle>() {
			public Rectangle call(String inputString) {
				// Read the file in an array of string object indicating each
				// point.
				String[] points = inputString.split(",");
				// Initialize the leftmost x and y coordinate
				Double leftMostUpperXCoord = Math.min(Double.parseDouble(points[1]), Double.parseDouble(points[3]));
				Double leftMostUpperYCoord = Math.max(Double.parseDouble(points[2]), Double.parseDouble(points[4]));
				// holds the upper left point for the rectangle
				Point upperLeftPoint = new Point(leftMostUpperXCoord, leftMostUpperYCoord);
				Double rightMostLowerXCoord = Math.max(Double.parseDouble(points[1]), Double.parseDouble(points[3]));
				Double rightMostLowerYCoord = Math.min(Double.parseDouble(points[2]), Double.parseDouble(points[4]));
				// holds the lower right point for the rectangle
				Point lowerRightPoint = new Point(rightMostLowerXCoord, rightMostLowerYCoord);
				return new Rectangle(Integer.parseInt(points[0]), upperLeftPoint, lowerRightPoint);
			}
		};
	}
}
