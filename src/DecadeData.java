
public class DecadeData {
	private int rating, releases;
	
	public DecadeData(MovieData data){
		this.rating = data.getAvgRating();
		this.releases = data.getNumReleased();
	}

	public int getRating() {
		return rating;
	}

	public int getReleases() {
		return releases;
	}

	public void addData(MovieData data) {
		this.releases += data.getNumReleased();
		this.rating = (this.rating + data.getAvgRating()) / 2;
	}
	
	
}
