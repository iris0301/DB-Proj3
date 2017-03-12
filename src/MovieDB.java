
/*****************************************************************************************
 * @file  MovieDB.java
 *
 * @author   John Miller
 */

import static java.lang.System.out;

import java.io.IOException;
import java.util.ArrayList;

/*****************************************************************************************
 * The MovieDB class makes a Movie Database.  It serves as a template for making other
 * databases.  See "Database Systems: The Complete Book", second edition, page 26 for more
 * information on the Movie Database schema.
 */
class MovieDB
{
    /*************************************************************************************
     * Main method for creating, populating and querying a Movie Database.
     * @param args  the command-line arguments
     */
    public static void main (String [] args)
    {
    	MovieDB movieDB = new MovieDB();
    	//movieDB.testFileList();
    	//movieDB.testProject();
    	movieDB.testMinus();
    	
    	
    	
    	
    	//movieDB.testUnion();
    	//movieDB.testJoins();
    	/**
        out.println ();

        Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                          "String Integer Integer String String Integer", "title year");

        Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                                            "String Integer Integer String String Integer", "title year");

        Table movieStar = new Table ("movieStar", "name address gender birthdate",
                                                  "String String Character String", "name");

        Table starsIn = new Table ("starsIn", "movieTitle movieYear starName",
                                              "String Integer String", "movieTitle movieYear starName");

        Table movieExec = new Table ("movieExec", "certNo name address fee",
                                                  "Integer String String Float", "certNo");

        Table studio = new Table ("studio", "name address presNo",
                                            "String String Integer", "name");

        Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
        Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
        out.println ();
        movie.insert (film0);
        movie.insert (film1);
        movie.insert (film2);
        movie.insert (film3);
        movie.print ();

        Comparable [] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
        out.println ();
        cinema.insert (film2);
        cinema.insert (film3);
        cinema.insert (film4);
        cinema.print ();

        Comparable [] star0 = { "Carrie_Fisher", "Hollywood", 'F', "9/9/99" };
        Comparable [] star1 = { "Mark_Hamill", "Brentwood", 'M', "8/8/88" };
        Comparable [] star2 = { "Harrison_Ford", "Beverly_Hills", 'M', "7/7/77" };
        out.println ();
        movieStar.insert (star0);
        movieStar.insert (star1);
        movieStar.insert (star2);
        movieStar.print ();

        Comparable [] cast0 = { "Star_Wars", 1977, "Carrie_Fisher" };
        out.println ();
        starsIn.insert (cast0);
        starsIn.print ();

        Comparable [] exec0 = { 9999, "S_Spielberg", "Hollywood", 10000.00 };
        out.println ();
        movieExec.insert (exec0);
        movieExec.print ();

        Comparable [] studio0 = { "Fox", "Los_Angeles", 7777 };
        Comparable [] studio1 = { "Universal", "Universal_City", 8888 };
        Comparable [] studio2 = { "DreamWorks", "Universal_City", 9999 };
        out.println ();
        studio.insert (studio0);
        studio.insert (studio1);
        studio.insert (studio2);
        studio.print ();

        movie.save ();
        cinema.save ();
        movieStar.save ();
        starsIn.save ();
        movieExec.save ();
        studio.save ();

        movieStar.printIndex ();

        //--------------------- project: title year

        out.println ();
        Table t_project = movie.project ("title year");
        t_project.print ();

        //--------------------- select: equals, &&
        
        out.println ();
        Table t_select = movie.select (t -> t[movie.col("title")].equals ("Star_Wars") &&
                                            t[movie.col("year")].equals (1977));
        t_select.print ();

        //--------------------- select: <

        out.println ();
        Table t_select2 = movie.select (t -> (Integer) t[movie.col("year")] < 1980);
        t_select2.print ();

        //--------------------- indexed select: key

        out.println ();
        Table t_iselect = movieStar.select (new KeyType ("Harrison_Ford"));
        t_iselect.print ();

        //--------------------- union: movie UNION cinema

        out.println ();
        Table t_union = movie.union (cinema);
        t_union.print ();

        //--------------------- minus: movie MINUS cinema

        out.println ();
        Table t_minus = movie.minus (cinema);
        t_minus.print ();

        //--------------------- equi-join: movie JOIN studio ON studioName = name

        out.println ();
        Table t_join = movie.join ("studioName", "name", studio);
        t_join.print ();

        //--------------------- natural join: movie JOIN studio

        out.println ();
        Table t_join2 = movie.join (cinema);
        t_join2.print ();
        **/
    } // main
    
    /**
     * 
    * @author Yunyun
    * @Description: test FileList.java
     */
    public void testFileList(){
    	out.println ();
        
    	//1.Test FileList.java   (1)add  (2)pack
    	Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                          "String Integer Integer String String Integer", "title year");
        Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        Comparable [] film1 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
        movie.insert(film0);
        movie.insert(film1);
        
        //2.Test FileList.java   (1)get  (2)unpack
        System.out.println("====");
        FileList tuples = (FileList) movie.getTuples();
        
        
        //3.Display
        for(int i =0;i<tuples.size();i++){
        	 Comparable[] tuple = tuples.get(i);
         	for(int j=0;j<tuple.length;j++){
         		System.out.print(tuple[j] + "  ");
         	}
         	System.out.println();
        }
        System.out.println("=============");
    }
    	
    
    
    
    
    /**
     * 
    * @author Yunyun
    * @Description: Test the Project Operation
    * @param     
    * @return void    
    * @throws
     */
    public void testProject(){
    	
    	//Test 1 movie "title year"
    	 out.println ();

         Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                           "String Integer Integer String String Integer", "title year");
         Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
         Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
         Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
         Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
         out.println ();
         movie.insert (film0);
         movie.insert (film1);
         movie.insert (film2);
         movie.insert (film3);
         movie.print ();
         
         movie.save ();
         
         //--------------------- project: title year

         out.println ();
         Table t1_project = movie.project ("title year");
         t1_project.print ();
        
         
         //Test 2 
         Table movieStar = new Table ("movieStar", "name address gender birthdate",
                 "String String Character String", "name");
         Comparable [] star0 = { "Carrie_Fisher", "Hollywood", 'F', "9/9/99" };
         Comparable [] star1 = { "Mark_Hamill", "Brentwood", 'M', "8/8/88" };
         Comparable [] star2 = { "Harrison_Ford", "Beverly_Hills", 'M', "7/7/77" };
         out.println ();
         movieStar.insert (star0);
         movieStar.insert (star1);
         movieStar.insert (star2);
         movieStar.print ();
         movieStar.save ();
         Table t2_project = movieStar.project ("name address birthdate");
         t2_project.print ();
         
         Table t3_project = movieStar.project ("name");
         t3_project.print ();
		
    }

    /**
     * 
    * @author Yunyun
    * @Description: Test the Minus Operatio
     */
    public void testMinus(){
    	//1.Init Table
    	out.println ();

        Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                          "String Integer Integer String String Integer", "title year");

        Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                                            "String Integer Integer String String Integer", "title year");

        Table movieStar = new Table ("movieStar", "name address gender birthdate",
                                                  "String String Character String", "name");
        Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
        Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
        out.println ();
        movie.insert (film0);
        movie.insert (film1);
        movie.insert (film2);
        movie.insert (film3);
        movie.print ();
        
        
        Comparable [] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
        out.println ();
        cinema.insert (film2);
        cinema.insert (film3);
        cinema.insert (film4);
        cinema.print ();

        Comparable [] star0 = { "Carrie_Fisher", "Hollywood", 'F', "9/9/99" };
        Comparable [] star1 = { "Mark_Hamill", "Brentwood", 'M', "8/8/88" };
        Comparable [] star2 = { "Harrison_Ford", "Beverly_Hills", 'M', "7/7/77" };
        out.println ();
        movieStar.insert (star0);
        movieStar.insert (star1);
        movieStar.insert (star2);
        movieStar.print ();
        movie.save ();
        cinema.save ();
        movieStar.save ();
        //--------------------- project: title year
        out.println ();
        
        //--------------------- minus: movie MINUS cinema
        out.println ();
        //Test 1
        Table t1_minus = movie.minus (cinema);
        t1_minus.print ();
        
        //Test 2
        Table t2_minus = movie.minus (movieStar);
        //t2_minus.print (); throw Exception
        
        
    }
    
    
    /**
     * @author Fu
     * Tests Union operation
     */
    
    public void testUnion(){
    	out.println ();
    	
        Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                          "String Integer Integer String String Integer", "title year");

        Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                                            "String Integer Integer String String Integer", "title year");

        Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
        Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
        out.println ();
        movie.insert (film0);
        movie.insert (film1);
        movie.insert (film2);
        movie.print ();
        
        
        Comparable [] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
        out.println ();
        cinema.insert (film3);
        cinema.insert (film3);
        cinema.insert (film3);
        cinema.print ();
        
        //--------------------- union: movie UNION cinema
        
        out.println ();
        Table t_union = movie.union (cinema);
        t_union.print ();
        
    }
    
    /**
     * author: Yongquan tan
     */
    public void testJoins(){
    	
    	   Table movie = new Table ("movie", "title year length genre studioName producerNo",
                   "String Integer Integer String String Integer", "title year");

    	   Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                     "String Integer Integer String String Integer", "title year");

    	   Table studio = new Table ("studio", "name address presNo",
                   "String String Integer", "name");
    	   Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
           Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
           Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
           Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
           out.println ();
           movie.insert (film0);
           movie.insert (film1);
           movie.insert (film2);
           movie.insert (film3);
           movie.print ();

           Comparable [] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
           out.println ();
           cinema.insert (film2);
           cinema.insert (film3);
           cinema.insert (film4);
           cinema.print ();
           
           Comparable [] studio0 = { "Fox", "Los_Angeles", 7777 };
           Comparable [] studio1 = { "Universal", "Universal_City", 8888 };
           Comparable [] studio2 = { "DreamWorks", "Universal_City", 9999 };
           out.println ();
           studio.insert (studio0);
           studio.insert (studio1);
           studio.insert (studio2);
           studio.print ();

    	 //--------------------- equi-join: movie JOIN studio ON studioName = name

        out.println ();
        Table t_join = movie.join ("studioName", "name", studio);
        t_join.print ();

        //--------------------- natural join: movie JOIN studio

        out.println ();
        Table t_join2 = movie.join (cinema);
        t_join2.print ();
        
    }
} // MovieDB class

