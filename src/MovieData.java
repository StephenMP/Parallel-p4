public class MovieData {
	private double avgRating;
	private int numReleased;

	public MovieData(String rating){
		this.avgRating = Double.parseDouble(rating);
		numReleased = 1;
	}
	
	public void addMovieData(String rating){
		double avgRating = Double.parseDouble(rating);
		this.avgRating = (avgRating + this.avgRating)/2;
		numReleased++;
	}

	public double getAvgRating() {
		return avgRating;
	}

	public int getNumReleased() {
		return numReleased;
	}
}
