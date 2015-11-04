package geospatial1.spatialQuery;

import java.io.Serializable;

import geospatial1.operation1.Point;

public class Rectangle implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8525368746044775440L;
	// holds unique id assigned to each rectangleint rectangleId;
	int rectangleId;
	// upper left point for the rectangle
	Point upperLeftPoint;
	// lower right point for the rectangle
	Point lowerRightPoint;
	
	public int getRectangleId() {
		return rectangleId;
	}

	public void setRectangleId(int rectangleId) {
		this.rectangleId = rectangleId;
	}

	public Point getUpperLeftPoint() {
		return upperLeftPoint;
	}

	public void setUpperLeftPoint(Point upperLeftPoint) {
		this.upperLeftPoint = upperLeftPoint;
	}

	public Point getLowerRightPoint() {
		return lowerRightPoint;
	}

	public void setLowerRightPoint(Point lowerRightPoint) {
		this.lowerRightPoint = lowerRightPoint;
	}

	// Constructor for initializing the rectangle object
	Rectangle(int inputId, Point inUpperLeftPoint, Point inLowerRightPoint) {
		this.rectangleId = inputId;
		this.upperLeftPoint = inUpperLeftPoint;
		this.lowerRightPoint = inLowerRightPoint;
	}

	// This method is used for determining whether the query window holds the
	// given rectangle or not.

	public Boolean isRectangleinsideQueryWindow(Rectangle rect) {
		Boolean isInside;
		// Check if the query window x and y coordinate for the upper left point
		// and
		// the lower right point enclose the given rectangle or not.
		if (upperLeftPoint.x <= rect.upperLeftPoint.x
				&& upperLeftPoint.y >= rect.upperLeftPoint.y
				&& lowerRightPoint.x >= rect.lowerRightPoint.x
				&& lowerRightPoint.y <= rect.lowerRightPoint.y) {
			isInside = true;
		} else {
			isInside = false;
		}
		return isInside;
	}

}
