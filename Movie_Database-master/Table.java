
/****************************************************************************************
 * @file  Table.java
 *
 * @author    Colby Eskew
 * @author    Micheal Tikala
 * @author    Joseph Zheng
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join. The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

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
        tuples    = new ArrayList <> ();
        index     = new TreeMap <> ();       // also try BPTreeMap, LinHashMap or ExtHashMap
        //index     = new LinHashMap <> (KeyType.class, Comparable [].class);

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
        //index     = new LinHashMap <> (KeyType.class, Comparable [].class);
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

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @author Colby Eskew
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        String [] attrs     = attributes.split (" ");
        Class []  colDomain = extractDom (match (attrs), domain);
        String [] newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;
        List <Comparable []> rows = new ArrayList <> ();
        
        

       for (Comparable [] row: this.tuples) {           //loops rows
              rows.add(this.extract(row, attrs));       //adds the columns
       } // for



        return new Table (name + count++, attrs, colDomain, newKey, rows);
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
     *
     * @param keyVal  the given key value
     * @author Colby Eskew
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();



       if (index.containsKey(keyVal)) {          
              rows.add(index.get(keyVal));       //adds rows
       } // if
       
       

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     * @author Joseph Zheng
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <Comparable []> ();

        for (Comparable [] tup : this.tuples) 
        {
        	rows.add(tup);       // adds rows
        } // for
        
        for (Comparable [] tup : table2.tuples) 
        {
        	rows.add(tup);       // adds rows
        } // for

        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     * @author Joseph Zheng
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <Comparable []> ();

        tuples.contains(rows);     // checks tuples

        for (Comparable [] tup : this.tuples)
    	{
    		if(!(table2.tuples.contains(tup)))
    		{
    			rows.add(tup);       // adds rows
    		} // if
        } // for

        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     * @author Michael Tilaka
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");

        List <Comparable []> rows = new ArrayList <> ();

        List<Comparable []> tuple1 = new ArrayList <Comparable []> ();
        List<Comparable []> tuple2 = new ArrayList <Comparable []> ();


        //Store tuples  in lists

        for (int i = 0; i < this.tuples.size(); i++){
            tuple1.add(this.tuples.get(i));

        }

        for (int i = 0; i < table2.tuples.size(); i++){
            tuple2.add(table2.tuples.get(i));

        }

        //Store col nums in arrays
        int [] attr1ColArr = new int[t_attrs.length];
        int [] attr2ColArr = new int[u_attrs.length];

        for (int i = 0; i < t_attrs.length; i++){
            attr1ColArr[i] = col(t_attrs[i]);

        }

        for (int i = 0; i < u_attrs.length; i++){
            attr2ColArr[i] = table2.col(u_attrs[i]);
        
        }

        boolean addEquality = true;

        //Concat like columns
        for (int i = 0; i < tuple1.size(); i++){
            for (int j = 0; j < tuple2.size(); j++){
                addEquality = true;

                for(int k = 0; k < attr1ColArr.length; k++){

                    //Duplicates
                    if (!(tuple1.get(i)[attr1ColArr[k]] == tuple2.get(j)[attr2ColArr[k]])) {
                        addEquality = false;

                    }

                }

                if(addEquality){
                    rows.add(ArrayUtil.concat(this.tuples.get(i), table2.tuples.get(j)));

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
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     * @author Michael Tilaka
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        List <Comparable []> rows = new ArrayList <> ();

        List <Integer> table1Matches = new ArrayList <> ();
        List <Integer> table2Matches = new ArrayList <> ();

        for (int i = 0; i < this.attribute.length; i++){
            for(int j = 0; j < table2.attribute.length; j++) {
                if(this.attribute[i].equals(table2.attribute[j])) {
                    table1Matches.add(i);
                    table2Matches.add(j);

                }

            }
            
        }

        for (int i = 0; i < this.tuples.size(); i++){
            outer:
            for (int j = 0; j < table2.tuples.size(); j++){
                for (int k = 0; k < table1Matches.size(); k++){
                    if (!(this.tuples.get(i)[table1Matches.get(k)].equals(table2.tuples.get(j)[table2Matches.get(k)]))) {
                        continue;

                    } else {
                            rows.add(this.tuples.get(i));
                            break outer;


                    }

                }

            }

        }

        // FIX - eliminate duplicate columns
        return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                                          ArrayUtil.concat (domain, table2.domain), key, rows);
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

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        out.print ("| ");
        for (String a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        for (Comparable [] tup : tuples) {
            out.print ("| ");
            for (Comparable attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
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
     * Check the size of the tuple (number of elements in list) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     * @author Joseph Zheng
     */
    private boolean typeCheck (Comparable [] t)
    { 
        if(t.length != this.attribute.length)
    	{
    		return false; //checks if the lengths from tuple and attribute matches
    	} // if
    	
    	int i = 0;
    
    	for (Comparable attr : t)
    	{
    		if (!(attr.getClass().equals(this.domain[i]))) 
        	{
    			if(attr.getClass().equals(Double.class) || this.domain[i].equals(Float.class))
    			{
    				i++;
    				continue; // checks if the data is either double or float values
    			} // if
        		return false; // false if there is a domain that is non-matching
            } // if   
        	i++; // increments i to indicate where we are in the array
    	} // for

        return true;
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

    public Table NonIndexSelect(KeyType keyVal) {
        ArrayList<Integer> keyIndexes = new ArrayList<Integer>();
        HashSet keyNames = new HashSet(Arrays.asList(key));
        for(int i = 0; i < attribute.length; i++){
            if(keyNames.contains(attribute[i])){
                keyIndexes.add(i);
            }
        }
        List<Comparable[]> rows = new ArrayList<>();
        for(int i = 0; i < tuples.size(); i++){
            Comparable[] currentTuple = tuples.get(i);
            List<Comparable> keyValues = new ArrayList<Comparable>();
            for(int j = 0; j < keyIndexes.size(); j++){
                keyValues.add(currentTuple[keyIndexes.get(j)]);
            }
            KeyType keyToCompare = new KeyType(keyValues.toArray(new Comparable[0]));
            if(keyToCompare.equals(keyVal)){
                rows.add(tuples.get(i));
            }
        }

        return new Table(name + count++, attribute, domain, key, rows);
    } // select 







/************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.
     *
     * @author  Joseph Zheng
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table NonIndexJoin (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");

        List <Comparable []> rows = new ArrayList <> ();

		if (t_attrs.length != u_attrs.length) {
			System.out.println("Cannot Perform Join Operator");
			return null;
		}
 		
		for (Comparable[] tuple1 : tuples) {
			for (Comparable[] tuple2 : table2.tuples) {
				
			    Comparable[] Attri1 = this.extract(tuple1, t_attrs);
			    Comparable[] Attri2 = table2.extract(tuple2, u_attrs);
			
			    boolean flag = true;
			    
				// Judge if attributes1 in table1 is equal to attributes2 in table 2
			    for (int i = 0; i < Attri1.length; i++) {
				
				    if (!Attri1[i].equals(Attri2[i])) {
						flag = false;
						break;
					}
				}

				// Concatenate tuples from table1&2 to form a new tuple
			    if (flag) {
				    Comparable[] join_tuple = ArrayUtil.concat(tuple1, tuple2);
				    rows.add(join_tuple);
				}
			}
		}

		// Disambiguate attribute names by append "2" to the end of any duplicate attribute name.
		// Here we just need to rename the attribute names in table2 then concatenate them to those in table1 
		String[] attribute2_new = table2.attribute;
		
		for (int j = 0; j < t_attrs.length; j++) {
			for (int k = 0; k < attribute2_new.length; ++k) {
				
				if (attribute2_new[k].equals(t_attrs[j])) {
					
					String tmp_attri = t_attrs[j] + "2"; 
					attribute2_new[j] = tmp_attri;
				}
			}
		}
		

		return new Table (name + count++, ArrayUtil.concat (attribute, attribute2_new),
				ArrayUtil.concat (domain, table2.domain), key, rows);
	} // join

} // Table class

