package api;


import java.util.Calendar;
import java.util.Scanner;
import java.sql.*;

public class TestQueries {
	
	DatabaseQueries app = new DatabaseQueries();
	Scanner scanner = new Scanner(System.in);
	int category;
	
	public void mainMenue ()  {
			System.out.println("You can choose a category from the following list:");
			System.out.println("[1] Movies");
			System.out.println("[2] Actors");
			System.out.println("[3] Directors");
			System.out.print("Please enter a number: ");
			category = scanner.nextInt();
			switch (category){
				case 1: moviesMenue(); break;
				case 2: actorsMenue(); break;
				case 3: directorsMenue(); break;
			}
	}
	
	public void moviesMenue () {
		System.out.println("What do you want to know?:");
		System.out.println("[1] 20 best rated movies");
		System.out.println("[2] Movies from last year with rating better than 6");
		System.out.println("[3] Actors acting in a Movie");
		System.out.println("[4] Directors of a Movie");
		System.out.println("[5] Rating of a Movie");
		System.out.println("[6] Genres of a Movie");
		System.out.println("[7] Back to MainMenue");
		System.out.print("Please enter a number: ");
		int moviesCat = scanner.nextInt();
		if (moviesCat<=2 || moviesCat == 7){
			switch(moviesCat) {
				case 1: System.out.print(app.bestMovies(20)); break;
				case 2: System.out.print(app.moviesRating(6,10,2014)); break; 
				case 7: mainMenue(); break;
			}
		} else {
			System.out.print("Title of the Movie: ");
			String title = scanner.next();
			switch(moviesCat){
				case 3: System.out.print(app.actorsInMovie(title)); break;
				case 4: System.out.print(app.directorsOfMovie(title)); break;
				case 5: System.out.print(app.ratingOfMovie(title)); break;
				case 6: System.out.print(app.genresOfMovie(title)); break;
			}
		}
		moviesMenue();
	}
	
	
	
	public void actorsMenue () {
		System.out.println("What do you want to know?");
		System.out.println("[1] Movies of an actor");
		System.out.println("[2] Debut of an actor");
		System.out.println("[3] Back to MainMenue");
		System.out.print("Please enter a number: ");
		int moviesCat = scanner.nextInt();
		if (moviesCat != 3) {
			System.out.print("Actor's firstname: ");
			String actorFName = scanner.next();
			System.out.print("Actor's lastname: ");
			String actorLName = scanner.next();
			switch (moviesCat) {
				case 1: System.out.print(app.moviesOfActor(actorFName, actorLName)); break;
				case 2: System.out.print(app.debutOfActor(actorFName, actorLName)); break;
			}
		} else { mainMenue(); }
		actorsMenue();
	}
	
	
	public void directorsMenue () {
		System.out.println("What do you want to know?:");
		System.out.println("[1] Movies of a Director");
		System.out.println("[2] Actors worked most frequently for a direcor");
		System.out.println("[3] Director with most movies per year");
		System.out.println("[4] Back to MainMenue");
		System.out.print("Please enter a number: ");
		int directorsCat = scanner.nextInt();
		if (directorsCat <=2) {
			System.out.print("Directors's firstname: ");
			String directorFName = scanner.next();
			System.out.print("Directors's lastname: ");
			String directorLName = scanner.next();
			switch (directorsCat){
				case 1: System.out.print(app.moviesOfDirector(
						directorFName, directorLName)); break;
				case 2: System.out.print(app.actorsWorkedForDirector(
						directorFName, directorLName)); break;
			}
		}  else {
			switch (directorsCat){
				case 3:
				System.out.print("Please enter a year: ");
				int year = scanner.nextInt();
				System.out.print(app.directorWithMostMovies(year)); break;
				case 4: mainMenue(); break;
			}
		}
		directorsMenue();
	}


	public static void main(String[] args) {
		TestQueries appTest = new TestQueries();
		appTest.mainMenue();
	}

}
