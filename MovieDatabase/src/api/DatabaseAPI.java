package api;

public interface DatabaseAPI {
	
	public String bestMovies (int count);
	
	public String moviesRating (int from, int to);
	
	public String moviesRating (int from, int to, int year);
	
	public String actorsInMovie (String movieTitle);
	
	public String directorsOfMovie (String movieTitle);
	
	public String ratingOfMovie (String movieTitle);
	
	public String genresOfMovie (String movieTitle);
	
	public String movieInfos (String movieTitle);
	
	public String moviesOfActor (String fname, String lName);
	
	public String moviesOfActor (String fname, String lName, int year);
	
	public String debutOfActor (String fName, String lName);
	
	public String  moviesOfDirector (String fName, String lName);
	
	public String  moviesOfDirector (String fName, String lName, int year);
	
	public String actorsWorkedForDirector (String fName, String lName);
	
	public String directorWithMostMovies (int year);
	
	

}
