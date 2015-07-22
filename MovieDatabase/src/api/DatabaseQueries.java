package api;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class DatabaseQueries implements DatabaseAPI{
	
	private String server = "localhost";
	private String port = "5432";
	private String dbName = "movies";
	private String url = "jdbc:postgresql://" + server + ":" + port + "/" + dbName;
	private String user = ""; //eigenen Benutzernamen eingeben
	private String password = ""; //eigenes Passwort eingeben
	
	private ResultSet manageQuery (String sql, TupleQ[] pstV)  {
		ResultSet rs = null;
		try {
			Class.forName("org.postgresql.Driver");
			Connection conn = DriverManager.getConnection(url, user, password);
			PreparedStatement pst = conn.prepareStatement(sql);
			for (int i=0; i<pstV.length; i++) {
				if (pstV[i].v().getClass() == (new Integer(1).getClass())) {
					
					pst.setInt(pstV[i].i(), (int)pstV[i].v());
				}
				else if (pstV[i].v().getClass() == (new Double(1.0).getClass())) {
					pst.setDouble(pstV[i].i(), (double)pstV[i].v());
				}
				else if (pstV[i].v().getClass() == (new String("").getClass())) {
					pst.setString(pstV[i].i(), (String)pstV[i].v());
				}
			}
			rs = pst.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}	
		return rs;
	}
	
	private String resultTable (ResultSet rs, TupleR[] col) {
		String tableString = "";
		try {
			while (rs.next()) {
				for (int i=0; i<col.length; i++){
					if (col[i].cl() == int.class) {
						int valueInt = rs.getInt(col[i].col());
						tableString += valueInt;
					}
					else if (col[i].cl() == double.class) {
						double valueDouble = rs.getDouble(col[i].col());
						tableString += valueDouble;
					}
					else if (col[i].cl() == String.class) {
						String valueString = rs.getString(col[i].col());
						tableString += valueString;
					}
					if (i<col.length-1) { tableString += "\t"; }
					else { tableString += "\n"; }
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	
		return tableString;
	}
	
	public String bestMovies (int count) {
		String sql = 	 ("SELECT title, rating "
						+ "FROM movies "
						+ "WHERE rating > 0.0 "
						+ "ORDER BY rating DESC "
						+ "LIMIT ?");
		TupleQ[] pstValues = {new TupleQ(1,count)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class), 
				new TupleR( "rating", double.class) });	
		return retString;
	}
	
	public String moviesRating (int from, int to) {
		String sql = 	 ("SELECT title, rating "
						+ "FROM movies "
						+ "WHERE rating >= ? "
						+ "AND rating <= ? "
						+ "ORDER BY rating DESC ");
		TupleQ[] pstValues = {new TupleQ(1,from), new TupleQ(2,to)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class), 
				new TupleR( "rating", double.class) });	
		return retString;
	}
	
	public String moviesRating (int from, int to, int year){
		String sql = 	("SELECT title, rating "
						+ "FROM movies "
						+ "WHERE rating >= ? "
						+ "AND rating <= ? "
						+ "AND rel_year = ? "
						+ "ORDER BY rating DESC ");
		TupleQ[] pstValues = {new TupleQ(1,from), 
				new TupleQ(2,to), new TupleQ(3, year)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class), 
				new TupleR( "rating", double.class) });	
		return retString;
	}
	
	public String actorsInMovie (String movieTitle){
		String sql = 	("SELECT firstname, lastname "
						+ "FROM movies, acts_in "
						+ "WHERE title = ? "
						+ "AND id = movieID ");
		TupleQ[] pstValues = {new TupleQ(1,movieTitle)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("firstname",String.class), 
				new TupleR( "lastname", String.class) });	
		return retString;
	}
	
	public String directorsOfMovie (String movieTitle){
		String sql = 	("SELECT firstname, lastname "
						+ "FROM movies, directs "
						+ "WHERE title = ? "
						+ "AND id = movieID ");
		TupleQ[] pstValues = {new TupleQ(1,movieTitle)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("firstname",String.class), 
				new TupleR( "lastname", String.class) });	
		return retString;
	}
	
	public String ratingOfMovie (String movieTitle) {
		String sql = 	("SELECT title, rating "
						+ "FROM movies "
						+ "WHERE title = ? ");
		TupleQ[] pstValues = {new TupleQ(1,movieTitle)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class), 
				new TupleR( "rating", double.class) });	
		return retString;	
	}
	
	public String genresOfMovie (String movieTitle) {
		String sql = 	("SELECT genre "
						+ "FROM movies, has_genre "
						+ "WHERE title = ? "
						+ "AND id = movieID");
		TupleQ[] pstValues = {new TupleQ(1,movieTitle)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("genre",String.class)});	
		return retString;	
	}
	
	public String movieInfos (String movieTitle){
		String sql = 	("SELECT title, rel_year, rating, rating_count, duration  "
						+ "FROM movies "
						+ "WHERE title = ? ");
		TupleQ[] pstValues = {new TupleQ(1,movieTitle)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class), 
				new TupleR( "rel_year", int.class),
				new TupleR( "rating", double.class),
				new TupleR( "rating_count", int.class),
				new TupleR( "duration", int.class)});	
		return retString;	
	}
	
	public String moviesOfActor (String fName, String lName){
		String sql = 	("SELECT title "
						+ "FROM movies, acts_in "
						+ "WHERE firstname = ? "
						+ "AND lastname = ? "
						+ "AND id = movieID" );
		TupleQ[] pstValues = {new TupleQ(1,fName), new TupleQ(2, lName)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class)});	
		return retString;	
	}
	
	public String moviesOfActor (String fName, String lName, int year){
		String sql = 	("SELECT title "
						+ "FROM movies, acts_in "
						+ "WHERE firstname = ? "
						+ "AND lastname = ? "
						+ "AND rel_year = ? "
						+ "AND id = movieID");
		TupleQ[] pstValues = {new TupleQ(1,fName), 
				new TupleQ(2, lName), new TupleQ(3, year)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class)});	
		return retString;	
	}
	
	public String debutOfActor (String fName, String lName){
		String sql = 	("SELECT title, rel_year "
						+ "FROM movies, acts_in "
						+ "WHERE firstname = ? "
						+ "AND lastname = ? "
						+ "AND id = movieID "
						+ "AND rel_year > 0 "
						+ "ORDER BY rel_year ASC "
						+ "LIMIT 1 ");
		TupleQ[] pstValues = {new TupleQ(1,fName), new TupleQ(2, lName)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class),
					new TupleR("rel_year",int.class)});	
		return retString;	
	}
	
	public String  moviesOfDirector (String fName, String lName){
		String sql = 	("SELECT title "
						+ "FROM movies, directs "
						+ "WHERE firstname = ? "
						+ "AND lastname = ? "
						+ "AND id = movieID");
		TupleQ[] pstValues = {new TupleQ(1,fName), new TupleQ(2, lName)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class)});	
		return retString;	
	}
	
	public String  moviesOfDirector (String fName, String lName, int year){
		String sql = 	("SELECT title "
						+ "FROM movies, directs "
						+ "WHERE firstname = ? "
						+ "AND lastname = ? "
						+ "AND rel_year = ? "
						+ "AND id = movieID");
		TupleQ[] pstValues = {new TupleQ(1,fName), 
				new TupleQ(2, lName), new TupleQ(3, year)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("title",String.class)});	
		return retString;	
	}
	
	public String actorsWorkedForDirector (String fName, String lName){
			String sql = 	("SELECT A.firstname, A.lastname "
							+ "FROM movies, acts_in A, directs D "
							+ "WHERE D.firstname = ? "
							+ "AND D.lastname = ? "
							+ "AND D.movieID = id "
							+ "AND A.movieID = id ");
			TupleQ[] pstValues = {new TupleQ(1,fName), new TupleQ(2, lName)};
			ResultSet rs = manageQuery (sql, pstValues);
			String retString = resultTable(rs, new TupleR[]
					{new TupleR("firstname",String.class), 
					new TupleR( "lastname", String.class) });	
			return retString;
	}
		
	
	public String directorWithMostMovies (int year) {
		String sql = 	("SELECT firstname, lastname, COUNT(*) AS number "
						+ "FROM movies, directs "
						+ "WHERE rel_year = ? "
						+ "AND id = movieID "
						+ "GROUP BY firstname, lastname "
						+ "ORDER BY COUNT(*) DESC "
						+ "LIMIT 1 " );
		TupleQ[] pstValues = {new TupleQ(1,year)};
		ResultSet rs = manageQuery (sql, pstValues);
		String retString = resultTable(rs, new TupleR[]
				{new TupleR("firstname",String.class), 
				new TupleR( "lastname", String.class),
				new TupleR( "number", String.class)});	
		return retString;
	}
}
