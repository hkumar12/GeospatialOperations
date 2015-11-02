package geospatial1.operation1;

import java.io.Serializable;

public class PointPair implements Comparable<PointPair>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Point p1;
	Point p2;

	public PointPair(Point point1, Point point2) {
		this.p1 = point1;
		this.p2 = point2;
	}

	public Point getP1() {
		return p1;
	}

	public void setP1(Point p1) {
		this.p1 = p1;
	}

	public Point getP2() {
		return p2;
	}

	public void setP2(Point p2) {
		this.p2 = p2;
	}

	public double getDistance() {
		return Math.sqrt(Math.pow(p2.getY() - p1.getY(), 2)
				+ Math.pow(p2.getX() - p1.getX(), 2));
	}

	public boolean isSamePoint() {
		if (this.p1.getX() == this.p2.getX()
				&& this.p1.getY() == this.p2.getY()) {
			return true;
		}
		return false;
	}

	public int compareTo(PointPair o) {
		// TODO Auto-generated method stub
		if (this.getDistance() - o.getDistance() > 0.0) {
			return 1;
		}
		return -1;
	}
}
