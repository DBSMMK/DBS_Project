
package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class DatabaseAdmin {

	public static void main(String[] args) {
		//Hier eure Postgre Datenbank einfuegen
		DatabaseAdmin admin = new DatabaseAdmin("jdbc:postgresql://localhost:5432/postgres","postgres","passwort");
		//erstellt den erforderlichen Table "movies", falls noch nicht vorhanden
		admin.createTables();

		try {
			//crawlt die Daten aus der CSV Datei, die wir bekommen haben
			admin.crawlMovieData("E:/Users/Marius/Downloads/imdb_top100t_2015-06-18.csv");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String databaseURL,user,password;

	public DatabaseAdmin(String databaseURL, String user, String password){
		this.databaseURL = databaseURL;
		this.user = user;
		this.password = password;
	}


	public void createTables(){
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager
					.getConnection(this.databaseURL,
							this.user, this.password);
			System.out.println("Datenbank erfolgreich geöffnet");

			stmt = c.createStatement();
			String sql =

				"CREATE TABLE IF NOT EXISTS movies \r\n" + 
				"	(id 		VARCHAR(9) PRIMARY KEY NOT NULL,\r\n" + 
				"	 title          TEXT NOT NULL,\r\n" + 
				"	 rel_year       INT2 NOT NULL,\r\n" + 
				"	 rating         NUMERIC(3,1),\r\n" + 
				"	 rating_count   INT,\r\n" + 
				"	 duration 	INT2,\r\n" + 
				"	 director_fname VARCHAR(127),\r\n" + 
				"	 director_lname VARCHAR(127));\r\n" + 
				"\r\n" + 
				"CREATE TABLE IF NOT EXISTS actors    \r\n" + 
				"	(firstname VARCHAR(127) NOT NULL,    \r\n" + 
				"	 lastname VARCHAR(127) NOT NULL,   \r\n" + 
				"	 PRIMARY KEY (firstname,lastname));\r\n" + 
				"\r\n" + 
				"			  \r\n" + 
				"CREATE TABLE IF NOT EXISTS directors       \r\n" + 
				"	(firstname VARCHAR(127) NOT NULL,    \r\n" + 
				"	 lastname VARCHAR(127) NOT NULL,   \r\n" + 
				"	 PRIMARY KEY (firstname,lastname));\r\n" + 
				"			  \r\n" + 
				"CREATE TABLE IF NOT EXISTS genres    \r\n" + 
				"	(name VARCHAR(255) PRIMARY KEY NOT NULL);\r\n" + 
				"			  \r\n" + 
				"CREATE TABLE IF NOT EXISTS acts_in    \r\n" + 
				"	(firstname VARCHAR(127) NOT NULL,   \r\n" + 
				"	lastname VARCHAR(127) NOT NULL,   \r\n" + 
				"	movieID VARCHAR(9) NOT NULL,   \r\n" + 
				"	PRIMARY KEY (movieID,firstname,lastname),    \r\n" + 
				"	FOREIGN KEY (movieID) REFERENCES movies(id),   \r\n" + 
				"	FOREIGN KEY (firstname,lastname) REFERENCES actors (firstname,lastname));\r\n" + 
				"			  \r\n" + 
				"CREATE TABLE IF NOT EXISTS has_genre    \r\n" + 
				"	(movieID VARCHAR(9) NOT NULL,     \r\n" + 
				"	genre VARCHAR(255) NOT NULL,    \r\n" + 
				"	PRIMARY KEY (movieID, genre),    \r\n" + 
				"	FOREIGN KEY (movieID) references movies(id),    \r\n" + 
				"	FOREIGN KEY (genre) references genres(name));";

			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(-1);
		}
		System.out.println("Movies-Table erfolgreich erstellt");
	}


	public void crawlMovieData(String file) throws Exception{

		BufferedReader reader = new BufferedReader( new FileReader (file));
		String         line = null;

		Class.forName("org.postgresql.Driver");
		Connection c = null;
		Statement stmt = null;
		c = DriverManager.getConnection(this.databaseURL,this.user, this.password);
		c.setAutoCommit(false);
		System.out.println("Opened database successfully");
		//Maximale anzahl an insert Statement pro Anweisung (Bei zu langem Statement wirft Postgre einen Error)
		int commitSize = 5;
		//Lese Zeile fuer Zeile die Datei und speichere die Zeile in "line"
		
		try {
			for (int i=0;(line = reader.readLine() ) != null;i++) {
				if (i%commitSize==0){

					if (i!=0) stmt.close();
					stmt = c.createStatement();
					c.commit();
					
				}
				//Parse die Spalten (Attribute)
				String[] attr = line.split("\t");
				//
				//Mache die Attribute tauglich fuer unsere Tables
				String id = attr[0];
				String title = attr[1].replaceAll("'", "''");;
				String rel_year = this.parseIntIfPossible(attr[2].substring(0, 4));
				String rating = this.parseFloatIfPossible(attr[3]);
				String rating_count = this.parseIntIfPossible(attr[4]);
				String duration = attr[5].indexOf(' ')>=0?attr[5].substring(0,attr[5].indexOf(' ')):attr[5];
				duration = this.parseIntIfPossible(duration);
				
				//Erstelle die director eintraege 
				String[] directorName = splitName(attr[6].replaceAll("'", "''"));
				this.insertDirector(id, directorName, c);

				
				//Erstelle den movie eintrag
				String sql = "INSERT INTO movies (id,title,rel_year,rating,rating_count,duration,director_fname,director_lname) VALUES "+
					          "('"+id+"','"+title+"',"+rel_year+","+rating+","+rating_count+","+duration+",'"+directorName[0]+"','"+directorName[1]+"'); ";
				stmt.executeUpdate(sql);
				System.out.println(sql);
				//Erstelle die genre eintraege und die has_genre beziehung
				String[] genres = attr[8].replaceAll("'", "''").split("\\|");
				this.insertGenres(id, genres, c);
				


				//Erstelle die actor eintraege und die acts_in beziehung
				String[] actors = attr[7].replaceAll("'", "''").split("\\|");
				this.insertActors(id, actors, c);

			}
			c.close();
			reader.close();
		} catch (Exception e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			//System.exit(-1);
		}
	}

	private String[] splitName(String fullname){
		int seperator = fullname.indexOf(' ');
		String[] name = new String[2];
		if (seperator>=0 && seperator < fullname.length()-1){
			name[0]=fullname.substring(0, seperator);
			name[1] = fullname.substring(seperator+1);
		} else {
			name[0]=fullname;
			name[1]="";
		}

		return name;
	}

	private void insertDirector(String movieID, String[] name,Connection c){
		try {
			Statement stmt = c.createStatement();
			String sql = 
					"INSERT INTO directors"+
							"(firstname, lastname)"+
							" SELECT '"+name[0]+"', '"+name[1]+"'"+
							"WHERE NOT EXISTS ("+
							" SELECT firstname,lastname FROM directors WHERE firstname = \'"+name[0]+"\' AND lastname = \'"+name[1]+"\'"+
							" ); ";

			stmt.executeUpdate(sql);
			stmt.close();
			c.commit();
			System.out.println("\t"+sql);
		} catch (Exception e){
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			//System.exit(-1);
		}
	}

	private void insertActors(String movieID, String[] fullnames, Connection c){

		String sql="";
		try {

			Statement stmt = c.createStatement();
			for (String fullname : fullnames){
				String[] name = this.splitName(fullname);
				sql += "INSERT INTO actors"+
						"(firstname, lastname)"+
						" SELECT '"+name[0]+"', '"+name[1]+"'"+
						"WHERE NOT EXISTS ("+
						" SELECT firstname,lastname FROM actors WHERE firstname = '"+name[0]+"' AND lastname = '"+name[1]+"'"+
						" ); ";
				
				sql += "INSERT INTO acts_in"+
				"(firstname,lastname,movieID)"+
				" SELECT '"+name[0]+"','"+name[1]+"','"+movieID+"'"+
				"WHERE NOT EXISTS ("+
				" SELECT firstname,lastname,movieID FROM acts_in WHERE firstname = '"+name[0]+"' AND lastname = '"+name[1]+"' AND movieID = '"+movieID+"' ); ";
				
			}
			stmt.execute(sql);
			stmt.close();
			c.commit();
			System.out.println("\t"+sql);
		} catch (SQLException e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			//System.exit(-1);
		}

	}

	private void insertGenres(String movieID, String[] genres, Connection c){

		String sql="";
		try {

			Statement stmt = c.createStatement();
			for (String genre : genres){
				sql += "INSERT INTO genres"+
						"(name)"+
						" SELECT '"+genre+"'"+
						"WHERE NOT EXISTS ("+
						" SELECT name FROM genres WHERE name = '"+genre+"'"+
						" ); ";
				
				sql += "INSERT INTO has_genre"+
				"(movieID,genre)"+
				" SELECT '"+movieID+"', '"+genre+"'"+
				"WHERE NOT EXISTS ("+
				" SELECT movieID,genre FROM has_genre WHERE movieID = '"+movieID+"' AND genre = '"+genre+"'); ";
				
			}
			stmt.execute(sql);
			stmt.close();
			c.commit();
			System.out.println("\t"+sql);
		} catch (SQLException e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			//System.exit(-1);
		}

	}

	//Hilfsfunktionen_________________________________________
	private String parseIntIfPossible(String s){
		String out;
		try {
			out = ""+Integer.parseInt(s);
		} catch (Exception e) {
			out = "NULL";
		}
		return out;
	}

	private String parseFloatIfPossible(String s){
		String out;
		try {
			out = ""+Float.parseFloat(s);
		} catch (Exception e) {
			out = "NULL";
		}
		return out;
	}

	private String strArrayToPostgreArray(String[] strAr){
		String out = "{";
		for (int i=0;i<strAr.length;i++){
			out+="\""+strAr[i]+"\""+((i==strAr.length-1)?"":",");
		}
		out += "}";
		return out;
	}

}


