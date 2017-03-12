
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import java.util.Queue;
import static java.lang.Boolean.*;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;
	//private static final String DIR ="";

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;


	/** Primary key. 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple number).
     */
    private final Map <KeyType, Comparable []> index;
    
    


    //----------------------------------------------------------------------------------
    // Constructors
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        
        
        //tuples    = new ArrayList <> ();
        tuples = new FileList(_name, 100 , domain);
        //System.out.println(computeRecordSize(_domain));
        index     = new TreeMap <> ();       // also try BPTreeMap, LinHashMap or ExtHashMap

    } // constructor
    
    

    
    
    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuple      the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = new TreeMap <> ();       // also try BPTreeMap, LinHashMap or ExtHashMap
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param name        the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     */
    public Table (String name, String attributes, String domains, String _key)
    {
        this (name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /**
    * Project the tuples onto a lower dimension by keeping only the given attributes.
    * Check whether the original key is included in the projection.
    *
    * #usage movie.project ("title year studioNo")
    * @author Yunyun
    * @Description: TODO
    * @param the attributes to project onto   
    * @return a table of projected tuples  
    * @throws
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        String [] attrs     = attributes.split (" "); //title year
        Class []  colDomain = extractDom (match (attrs), domain); //String Interger
        String [] newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs; //title year
        
        //List <Comparable []> rows = new ArrayList <> ();
        int temp = count++;
        List <Comparable []> rows = new FileList(name + temp, 100, colDomain);
        int attrsLength = attrs.length;
        //1.key arributes index in old_tuples
        int[] oldTuplesIndex = new int[attrsLength];
        for(int i=0;i<attrs.length;i++){
        	for(int j =0;j< this.attribute.length;j++){
        		if(  this.attribute[j].equals(attrs[i])  ){
        			oldTuplesIndex[i] = j;
        			//System.out.println(oldTuplesIndex[i]);
        		}
        	}
        }
        
        
        Comparable[] newTuple;
        //2.traverse every row in table
        for (Comparable[] oldTuple : tuples) {
            //3. take the vale of every tuple
        	newTuple = new Comparable[attrsLength];
        	for (int i=0;i<attrs.length;i++){
        		newTuple[i] = oldTuple[oldTuplesIndex[i]];
            }
        	rows.add(newTuple);
        } 
        
        
        //name  attribute(attributes names) domain  key   the primary key
        return new Table (name + temp, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     * @author: Sahisnu Nimmakayalu
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();

        Comparable [] holder; 
	//used to hold the row temporarily. 
       
        holder = index.get(keyVal); 
	//get the comparable that holds the data of the row from the tree map. 
       
        if (holder!= null)
        {
    	   rows.add(holder); 
    	   //if found adds the holder row to the actual row. 
        }
	
	

        return new Table (name + count++, attribute, domain, key, rows);
	//return the table corresponding to the input key. 
	
    } // select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     * @author Fu
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        if(this.compatible(table2)){
        	for(int i = 0; i < this.tuples.size(); i++){
        		boolean flag1 = false, flag2 = false;
        		//rows.add(this.tuples.get(i));
        		/*for(int j = i; j > 0; j--){
        			if( matchTuple(this.tuples.get(i), table2.tuples.get(j)) ||
        				(matchTuple(this.tuples.get(i), this.tuples.get(j)) ) ||
        				(matchTuple(table2.tuples.get(i), table2.tuples.get(j)) ) 
        					){
        				flag = true;
        				continue;
        			}
        		}*/
        		for(int j = 1; j < rows.size(); j++){
            		if( matchTuple(this.tuples.get(i), rows.get(j))){
            			flag1 = true;
            			continue;
            		}
        			
        		}
        		if(flag1 == false){
        			rows.add(this.tuples.get(i));
        		}
        		for(int j = 0; j < rows.size(); j++){
        			if( matchTuple(table2.tuples.get(i), rows.get(j))){
            			flag2 = true;
            			continue;
            		}
        		}
        		if(flag2 == false)
        			rows.add(table2.tuples.get(i));
        	}
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // union


    /**
     * Minus operator will output the tuples that do not exist in the second table. 
     * #usage movie.minus (show)
     * @author Yunyun
     * @Description: Take the difference of this table and table2. Check that the two tables are compatible.
     * @param @param table2 The rhs table in the minus operation   
     * @return a table representing the difference 
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        
        int temp = count++;
        List <Comparable []> rows = new FileList(name + temp, 100, this.domain);
        //List <Comparable []> rows = new ArrayList <> ();

        //1.Check that the two tables are compatible.
        if(this.compatible(table2)){
        	int attributeLen = attribute.length;
        	//2. Match we macth all tuples  Traverse
        	for(int i=0;i<this.tuples.size();i++){
        		boolean flag = false; 
        		for(int j=0;j<table2.tuples.size();j++){
        			//System.out.println(i+""+j+" "+matchTuple(this.tuples.get(i),table2.tuples.get(j)));
        			if(  matchTuple(  this.tuples.get(i),table2.tuples.get(j)  )  ){
        				flag = true;
        				continue;
        			}
        		}
        		//System.out.println(flag);
        		if(flag == false){
        			rows.add(this.tuples.get(i));
        		}
        	}
        }
        

        return new Table (name + temp, attribute, domain, key, rows);
    } // minus
    
    /**
    * 
    * @author Yunyun
    * @Description: judge whether tuple1 equals tuple2
    * @param @param tuple1 
    * @param @param tuple2
    * @return boolean  tuple1 == tuple2 :true  else return false
     */
    private boolean matchTuple(Comparable[] tuple1,Comparable[] tuple2){
    	//Match we macth all the attr in the tuple (also we can macth the primary key first)
    	for(int i= 0 ;i<tuple1.length;i++){  
    		if(!tuple1[i].equals(tuple2[i])){
    			return false;
    		}
    	}
    	return true;
    }

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.
     *
     * #usage movie.join ("studioNo", "name", studio)
     * @author: Sahisnu Nimmakayalu 
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {	
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");
        //splits the input parameters. 

        List <Comparable []> rows = new ArrayList <> ();
     		
        int [] attHolder1 = new int [t_attrs.length]; 
        int [] attHolder2 = new int [u_attrs.length]; 
        //used to hold the columns for the interation through tables. 
        //they technically have to be the same length for any comparison to happen. 
        		
        		
        for (int i = 0; i<attHolder1.length; i++)
        {
        	int temp = this.col(t_attrs[i]); 
        	
        	attHolder1[i] = temp; 
        	//System.out.println("test1 " +attHolder1[i] + t_attrs[i]); 
        }
        //gets the column index for the wanted columns for table 1.
        
        
        for (int i = 0; i<attHolder2.length; i++)
        {
        	int temp = table2.col(u_attrs[i]); 
        	
        	attHolder2[i] = temp; 
        	
        	//System.out.println("test2 "+attHolder2[i] + u_attrs[i]);
        }       
        //gets the column index for the wanted columns. 
        
        List <Comparable []> temp1Tuple = this.tuples; 
        List <Comparable []> temp2Tuple = table2.tuples;
        //temporary holds the tuples due to fact that tuples will be modified and we do not want to modify the actual tuple.
        
        boolean isTrue = false; 
        
        Comparable [] tempRow = null; 
        //holds the row that is added to new table. 
        
        for (Comparable [] temp1 : temp1Tuple)
        {
        	for (Comparable [] temp2 : temp2Tuple)
        	{
        		//iterate through both tuples. 
        		
        		for (int i = 0; i<attHolder1.length; i++)
        		{
        			if (temp1[attHolder1[i]].equals(temp2[attHolder2[i]]))
        			{
        				//check the tuple and if the attributes match up combine the tuples into 1 table add to row. 
        				tempRow = ArrayUtil.concat(temp1, Arrays.copyOfRange(temp2, 0, temp2.length)); 
        					if (i == attHolder1.length-1)
        						rows.add(tempRow); 
        						//used to make sure we check all keys then add. 
        			}
        			else
        			{
        				//if not matching element break out of row no need to examine anymore. 
        				break; 
        			}

        		}
        		

        	}
        	 
        }

        return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                                          ArrayUtil.concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *@author yongquan tan
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
        public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        List <Comparable []> rows = new ArrayList <> ();		//temp tuple holder
        List <Comparable []> finalRows = new ArrayList <> ();	//holds final tuples
        List<String> finalAttr = new ArrayList<String>();		//holds joined attributes
        List<Class> finalDom = new ArrayList<Class>();			//holds final domain values

        int primary = 0;
        int foreign = 0;
        List<String> joinedAttr = new ArrayList<String>();		//temp holder for joined attributes
        List<Class> joinedDom = new ArrayList<Class>();			//temp holder for joined domains

        //joins domain and attributes
         for(int i = 0 ; i < this.attribute.length; i++){
        	joinedAttr.add(this.attribute[i]);
        	joinedDom.add(this.domain[i]);

        	if(this.attribute[i].equals(table2.key[0])){
        		
        		foreign = i;
	        	for(int j = 0; j < table2.attribute.length; j++){
	        		joinedAttr.add(table2.attribute[j]);
	            	joinedDom.add(this.domain[j]);

	        	}
        	}
        }
         
         //remove duplicates
         for(int i = 0; i < joinedAttr.size(); i++){
     		for(int j = i+1; j < joinedAttr.size(); j++){
     			if(joinedAttr.get(i).equals(joinedAttr.get(j))){
     				i++;
     			}
     		}
     		finalAttr.add(joinedAttr.get(i));
     		finalDom.add(joinedDom.get(i));
     	} 

        //joins tuples
        List<Comparable> tempTuples;		//temp holder for tuples
        Comparable[] newTuples;
        
        for(Comparable[] e: this.tuples){        	
        	for(Comparable[] f: table2.tuples){ 
        		
        		if(e[foreign].equals(f[0])){
        			tempTuples = new ArrayList<Comparable>();
        			
        			for(int x = 0; x < e.length; x++){
        				tempTuples.add(e[x]);
        				if(x == foreign){
	        				for(int y = 0; y < f.length; y++){
	        					tempTuples.add(f[y]);
	        				}
        				}
        			}

        			newTuples = new Comparable[tempTuples.size()];
        			newTuples = tempTuples.toArray(newTuples);
        			rows.add(newTuples);
        		}
        	}
        }
        
        //remove duplicate tuples
        List<Comparable> tempSet;
        for(Comparable[] r: rows){
        	tempSet = new ArrayList<Comparable>();
        	for(int i = 0; i < r.length; i++){
        		for(int j = i+1; j < r.length; j++){
        			if(r[i].equals(r[j])){
        				i++;
        			}
        		}
    			tempSet.add(r[i]);

        	}
        	Comparable [] tempArray = tempSet.toArray(new Comparable[tempSet.size()]);
        	finalRows.add(tempArray);
        }

        return new Table (name + count++, finalAttr.toArray(new String[finalAttr.size()]),
        		finalDom.toArray(new Class[finalDom.size()]), key, finalRows);

    } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (int i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;  // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            Comparable [] keyVal = new Comparable [key.length];
            int []        cols   = match (key);
            for (int j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName


    /**
    * Print this table.
    * change the code style make it more easy to read 
    * @author Yunyun
    * @Description: TODO
    */
    public void print ()
    {
    	out.println ("\n Table " + name);
    	out.print ("|-");
        for (int i = 0; i < attribute.length; i++) {
        	out.print ("---------------");
        }
        out.println ("-|");
        
        //1. col name
        out.print ("| ");
        for (String a : attribute) {
        	out.printf ("%15s", a);
        }
        out.println (" |");
        
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) {
        	out.print ("---------------");
        }
        out.println ("-|");
        
        //2.tuples (row)
        for (Comparable [] tup : tuples) {
            out.print ("| ");
            // value in row
            for (Comparable attr : tup){
            	out.printf ("%15s", attr);
            }
            out.println (" |");
        } // for
        
        
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++){
        	out.print ("---------------");
        }
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        for (Map.Entry <KeyType, Comparable []> e : index.entrySet ()) {
            out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
        } // for
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

        
    
    
    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
        	//System.out.println("===="+ DIR + name + EXT);
            ObjectOutputStream oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (int j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (int j = 0; j < column.length; j++) {
            boolean matched = false;
            for (int k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        Comparable [] tup = new Comparable [column.length];
        int [] colPos = match (column);
        for (int j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) 
     * as well as 
     * the type of each value to ensure it is from the right domain. 
     *@author: Yongquan Tan
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
    	if(t.length == attribute.length){
    		for(int i = 0; i < t.length; i++){
    			if(t[i].getClass().equals(this.domain[i])){
    				continue;
    			}
    		}
    		return true;
    	}
    	else return false;
    } // typeCheck
  

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        Class [] classArray = new Class [className.length];

        for (int i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        Class [] obj = new Class [colPos.length];

        for (int j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

    
    /**
     * 
    * @author Yunyun
    * @Description: get the tuple
    * @return List<Comparable[]>    
    * @throws
     */
    public List<Comparable[]> getTuples() {
		return tuples;
	}
} // Table class
