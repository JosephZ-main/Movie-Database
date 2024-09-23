
 
/*****************************************************************************************
 * @file  TestTupleGenerator.java
 *
 * @author   Michael Tilaka and Joseph Zheng
 */

import static java.lang.System.out;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/*****************************************************************************************
 * This class tests the TupleGenerator on the Student Registration Database defined in the
 * Kifer, Bernstein and Lewis 2006 database textbook (see figure 3.6).  The primary keys
 * (see figure 3.6) and foreign keys (see example 3.2.2) are as given in the textbook.
 */
public class TestTupleGenerator
{

    private static Table Student_1000, Student_2000, Student_5000, Student_10000/*, Student_200, Student_500, Student_1000*/;
    private static Table Transcript_1000, Transcript_2000, Transcript_5000, Transcript_10000, Transcript_20000, Transcript_50000;
    private static List<Comparable[]> StudentTable, TranscriptTable;
    /*************************************************************************************
     * The main method is the driver for TestGenerator.
     * @param args  the command-line arguments
     */
    public static void main (String [] args)
    {
        var test = new TupleGeneratorImpl ();

        test.addRelSchema ("Student",
                           "id name address status",
                           "Integer String String String",
                           "id",
                           null);
        
        test.addRelSchema ("Professor",
                           "id name deptId",
                           "Integer String String",
                           "id",
                           null);
        
        test.addRelSchema ("Course",
                           "crsCode deptId crsName descr",
                           "String String String String",
                           "crsCode",
                           null);
        
        test.addRelSchema ("Teaching",
                           "crsCode semester profId",
                           "String String Integer",
                           "crcCode semester",
                           new String [][] {{ "profId", "Professor", "id" },
                                            { "crsCode", "Course", "crsCode" }});
        
        test.addRelSchema ("Transcript",
                           "studId crsCode semester grade",
                           "Integer String String String",
                           "studId crsCode semester",
                           new String [][] {{ "studId", "Student", "id"},
                                            { "crsCode", "Course", "crsCode" },
                                            { "crsCode semester", "Teaching", "crsCode semester" }});

        var tables = new String [] { "Student", "Professor", "Course", "Teaching", "Transcript" };
        var tups   = new int [] { 10000, 1, 1, 1, 10000 };
    
        var resultTest = test.generate (tups);

        Student_1000 = new Table ("Student","id name address status","Integer String String String", "id");
        Student_2000 = new Table ("Student","id name address status","Integer String String String", "id");
        Student_5000 = new Table ("Student","id name address status","Integer String String String", "id");
        Student_10000 = new Table ("Student","id name address status","Integer String String String", "id");
        //Student_1000 = new Table ("Student","id name address status","Integer String String String", "id");

        Transcript_1000 = new Table("Transcript", "studId crsCode semester grade", "Integer String String String", "studId crsCode semester");
        Transcript_2000 = new Table("Transcript", "studId crsCode semester grade", "Integer String String String", "studId crsCode semester");
        Transcript_5000 = new Table("Transcript", "studId crsCode semester grade", "Integer String String String", "studId crsCode semester");
        Transcript_10000 = new Table("Transcript", "studId crsCode semester grade", "Integer String String String", "studId crsCode semester");

        StudentTable = new ArrayList<Comparable[]>();
        TranscriptTable = new ArrayList<Comparable[]>();

        
        for (int i = 0; i < resultTest[4].length; i++) {
            if(i < 1000)
                Transcript_1000.insert(resultTest[4][i]);
            if(i < 2000)
                Transcript_2000.insert(resultTest[4][i]);
            if(i < 5000)
                Transcript_5000.insert(resultTest[4][i]);
            if(i < 10000)
                Transcript_10000.insert(resultTest[4][i]);

            TranscriptTable.add(resultTest[4][i]);
        }

        for (int i = 0; i < resultTest[0].length; i++) {
            if(i < 1000)
                Student_1000.insert(resultTest[0][i]);
            if(i < 2000)
                Student_2000.insert(resultTest[0][i]);
            if(i < 5000)
                Student_5000.insert(resultTest[0][i]);
            if(i < 10000)
                Student_10000.insert(resultTest[0][i]);
            /*if(i < 1000)
                Student_1000.insert(resultTest[0][i]);*/

            StudentTable.add(resultTest[0][i]);
        }

        long start = System.currentTimeMillis();
        Table t_join = Transcript_1000.NonIndexJoin("studId", "id", Student_1000);
        long end = System.currentTimeMillis();
        int join_1000 = (int)(end - start);

        start = System.currentTimeMillis();
        t_join = Transcript_2000.NonIndexJoin("studId", "id", Student_2000);
        end = System.currentTimeMillis();
        int join_2000 = (int)(end - start);

        start = System.currentTimeMillis();
        t_join = Transcript_5000.NonIndexJoin("studId", "id", Student_5000);
        end = System.currentTimeMillis();
        int join_5000 = (int)(end - start);

        start = System.currentTimeMillis();
        t_join = Transcript_10000.NonIndexJoin("studId", "id", Student_10000);
        end = System.currentTimeMillis();
        int join_10000 = (int)(end - start);

        System.out.println("1000: " + join_1000);
        System.out.println("2000: " + join_2000);
        System.out.println("5000: " + join_5000);
        System.out.println("10000: " + join_10000);

        /**
        //10 tuples for student select
        int count = 0;
        long start = System.currentTimeMillis();
        long end = 0;
        for (Comparable[] instance : StudentTable) {
            System.out.print("Select " + count + ": ");
            Table ti_select = Student_10.select(new KeyType(instance[0]));
            //ti_select.print();

            if (++count == 10) {
                end = System.currentTimeMillis();
                break;

            }

        }

        int select10 = (int)(end - start);

        //20 tuples
        count = 0;
        start = System.currentTimeMillis();
        for (Comparable[] instance : StudentTable) {
            System.out.print("Select " + count + ": ");
            Table ti_select = Student_20.select(new KeyType(instance[0]));
            //ti_select.print();

            if (++count == 20) {
                end = System.currentTimeMillis();
                break;

            }

        }
        int select20 = (int)(end - start);
        
        //50 Tuples
        count = 0;
        start = System.currentTimeMillis();
        for (Comparable[] instance : StudentTable) {
            System.out.print("Select " + count + ": ");
            Table ti_select = Student_50.select(new KeyType(instance[0]));
            //ti_select.print();

            if (++count == 50) {
                end = System.currentTimeMillis();
                break;

            }

        }
        int select50 = (int)(end - start);

        count = 0;
        start = System.currentTimeMillis();
        for (Comparable[] instance : StudentTable) {
            System.out.print("Select " + count + ": ");
            Table ti_select = Student_100.select(new KeyType(instance[0]));
            //ti_select.print();

            if (++count == 100) {
                end = System.currentTimeMillis();
                break;

            }

        }
        int select100 = (int)(end - start);

        count = 0;
        start = System.currentTimeMillis();
        for (Comparable[] instance : StudentTable) {
            System.out.print("Select " + count + ": ");
            Table ti_select = Student_1000.select(new KeyType(instance[0]));
            //ti_select.print();

            if (++count == 1000) {
                end = System.currentTimeMillis();
                break;

            }

        }
        int select1000 = (int)(end - start);

        System.out.println("10: " + select10);
        System.out.println("20: " + select20);
        System.out.println("50: " + select50);
        System.out.println("100: " + select100);
        System.out.println("1000: " + select1000);*/


    } // main


} // TestTupleGenerator

