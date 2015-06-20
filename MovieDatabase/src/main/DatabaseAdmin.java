
package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class DatabaseAdmin {
	
	public static void main(String[] args) {
		//Hier eure Postgre Datenbank einfuegen
		DatabaseAdmin admin = new DatabaseAdmin("jdbc:postgresql://localhost:5432/postgres","postgres","passwort");
		//erstellt den erforderlichen Table "movies", falls noch nicht vorhanden
		admin.createMoviesTable();
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

	
	public void createMoviesTable(){
		Connection c = null;
	    Statement stmt = null;
	    try {
	      Class.forName("org.postgresql.Driver");
	      c = DriverManager
	         .getConnection(this.databaseURL,
	         this.user, this.password);
	      System.out.println("Datenbank erfolgreich geöffnet");

	      stmt = c.createStatement();
	      String sql = "CREATE TABLE movies "+
	                   "(id 			VARCHAR(9) PRIMARY KEY NOT NULL," +
	                   " title          TEXT    NOT NULL," +
	                   " rel_year       INT2     NOT NULL," +
	                   " rating         NUMERIC(3,1)," +
	                   " rating_count   INT," +
	                   " duration 		INT2,"+
	      			   " director       VARCHAR(255)," +
	      			   " starring   	VARCHAR(255)[]," +
	      			   " genre   		VARCHAR(255)[])";
	      stmt.executeUpdate(sql);
	      stmt.close();
	      c.close();
	    } catch ( Exception e ) {
	      System.err.println( e.getClass().getName()+": "+ e.getMessage() );
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
         int commitSize = 10;
         //Lese Zeile fuer Zeile die Datei und speichere die Zeile in "line"
         for (int i=0;(line = reader.readLine() ) != null;i++) {
        	 try {
    			 if (i%commitSize==0){
    				 if (i!=0){
    					 c.commit();
        				 stmt.close();
    				 }
    				 stmt = c.createStatement();
    			 }
    			 	//Parse die Spalten (Attribute)
        			 String[] attr = line.split("\t");
        			 //Mache die Attribute tauglich fuer unsere Datenbank
        			 String _id = attr[0];
        			 String _title = attr[1].replaceAll("'", "''");;
        			 String _rel_year = this.parseIntIfPossible(attr[2].substring(0, 4));
        			 String _rating = this.parseFloatIfPossible(attr[3]);
        			 String _rating_count = this.parseIntIfPossible(attr[4]);
        			 String _duration = attr[5].indexOf(' ')>=0?attr[5].substring(0,attr[5].indexOf(' ')):attr[5];
        			 _duration = this.parseIntIfPossible(_duration);
        			 String _director = attr[6].replaceAll("'", "''");
        			 String _starring = strArrayToPostgreArray(attr[7].replaceAll("'", "''").split("\\|"));
        			 String _genre = strArrayToPostgreArray(attr[8].replaceAll("'", "''").split("\\|"));
        			 //Insert Statement
        			 String sql = "INSERT INTO movies (id,title,rel_year,rating,rating_count,duration,director,starring,genre) "
        					 + "VALUES (\'"+_id+"\',\'"+_title+"\',"+_rel_year+","+_rating+","+_rating_count+","+_duration+",\'"+_director+"\',\'"+_starring+"\',\'"+_genre+"\');";
        			 stmt.executeUpdate(sql);
        			 System.out.println(sql);

        	 } catch (Exception e) {
        		 System.err.println( e.getClass().getName()+": "+ e.getMessage() );
        		 System.exit(-1);
        	 }
         }
		 c.close();
         reader.close();
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


