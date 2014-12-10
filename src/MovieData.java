public class MovieData {
	private int avgRating, numReleased;

	public MovieData(int rating){
		this.avgRating = rating;
		numReleased = 1;
	}
	
	public void addMovieData(int rating){
		this.avgRating = (avgRating + rating)/2;
		numReleased++;
	}

	public int getAvgRating() {
		return avgRating;
	}

	public int getNumReleased() {
		return numReleased;
	}
}
