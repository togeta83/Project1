import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project1 {
    def main(args: Array[String]): Unit = {
        
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\Apps\\Spark\\spark-3.2.1-bin-hadoop3.2")
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1")    
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

        //This block to connect to mySQL
        /**************val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/p1" // Modify for whatever port you are running your DB on
        val username = "root"
        val password = "############" // Update to include your password
        var connection:Connection = null

        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)

        // Method to check login credentials
        val adminCheck = login(connection)
        if (adminCheck) {
            println("Welcome Admin! Loading in data...")
        } else {
            println("Welcome User! Loading in data...")
        }***********************/


        
        insertPopData(hiveCtx)


       


        var acctOpt = 0
        var accountType = ""
        var password = ""
        var scanner = new Scanner(System.in)
        
        println("Please select the account type to access:")
        println("[1]-[User] " + "[2]-[Administrator] " + "[0]-[Exit] ")
        acctOpt = scanner.nextInt()
        scanner.nextLine()
        while (acctOpt > 0 && acctOpt <3) {
        while(password != "password"){
        println("Please enter your password:")
        password = scanner.nextLine()
        }        
        if (acctOpt == 1) {
            accountType = "User"
            println("You chose " + accountType + " Account")
            userQuery(hiveCtx)
        } else if (acctOpt == 2) {
             accountType = "Administrator"
             println("You chose " + accountType + " Account.")
             adminQuery(hiveCtx)
            } 
        acctOpt = 0
        println("Please select the account type to access:")
        println("[1]-[User] " + "[2]-[Administrator] " + "[0]-[Exit] ")
        acctOpt = scanner.nextInt()
        scanner.nextLine()
        }
       println("Good bye..")

        sc.stop() 
    }

    
    // Returns true if admin, false if basic user, gets stuckl in a loop until correct combo is inputted (FIX)
    def login(connection: Connection): Boolean = {
        
        while (true) {
            val statement = connection.createStatement()
            val statement2 = connection.createStatement()
            println("Enter username: ")
            var scanner = new Scanner(System.in)
            var username = scanner.nextLine().trim()

            println("Enter password: ")
            var password = scanner.nextLine().trim()
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM admin_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet.next() ) {
                if (resultSet.getString(1) == "1") {
                    return true;
                }
            }

            val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM user_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet2.next() ) {
                if (resultSet2.getString(1) == "1") {
                    return false;
                }
            }

            println("Username/password combo not found. Try again!")
        }
        return false
    }

    def insertPopData(hiveCtx:HiveContext): Unit = {
           
        
        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            
            .load("input/NST-EST2021-alldata.csv")
        

       

        
        output.createOrReplaceTempView("temp_data")
        
        hiveCtx.sql("Drop TABLE popData")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS popData (REGION INT, State INT, Name STRING, POPESTIMATE2020 INT, " + 
        "POPESTIMATE2021 INT, Births2020 INT, Births2021 INT, Deaths2020 INT, Deaths2021 INT)")

        hiveCtx.sql("INSERT INTO popData SELECT REGION, State, Name, POPESTIMATE2020, POPESTIMATE2021, " + 
        "Births2020, Births2021, Deaths2020, Deaths2021 FROM temp_data")
        
        
        val summary = hiveCtx.sql("SELECT * FROM popData")
        println("summary.show().....: ")
        summary.show()
    }

    def userQuery(hiveCtx:HiveContext): Unit = {
        var option = 0
        var scanner = new Scanner(System.in)
        
        do{
                println("Please choose a set of query results: Enter 0 to quit.")
                println("[1]-[Top 10 States with the lowest number of deaths in 2021]")
                println("[2]-[Top 10 States with the highest number of deaths in 2021]")
                println("[3]-[Top 10 States with the highest number of births in 2021]")
                println("[4]-[Top 10 States with the highest population in 2021]")
                
                option = scanner.nextInt()
                scanner.nextLine()
                
                
                if (option == 1){
                var result = hiveCtx.sql("SELECT Name, DEATHS2021 FROM popData WHERE State > 0 ORDER BY DEATHS2021 ASC LIMIT 10")
                println("The least Deaths in 2021:")
                result.show()
                } else if (option == 2){
                var result = hiveCtx.sql("SELECT Name, DEATHS2021 FROM popData WHERE State > 0 ORDER BY DEATHS2021 DESC LIMIT 10")
                println("The most Deaths in 2021:")
                result.show()
                } else if (option == 3){
                var result = hiveCtx.sql("SELECT Name, Births2021 FROM popData WHERE State > 0 ORDER BY Births2021 DESC LIMIT 10")
                println("The most Births in 2021:")
                result.show()
                } else if (option == 4){
                var result = hiveCtx.sql("SELECT Name, POPESTIMATE2021 FROM popData WHERE State > 0 ORDER BY POPESTIMATE2021 DESC LIMIT 10")
                println("The highest populatlion in 2021:")
                result.show()
            }
           } while (option != 0)
        
        
    }

    def adminQuery(hiveCtx:HiveContext): Unit = {
        var option = 0
        var scanner = new Scanner(System.in)

        do{
                println("Please choose a set of query results: Enter 0 to quit.")
                println("[1]-[Top 10 States with the lowest number of deaths in 2021]")
                println("[2]-[Top 10 States with the highest number of deaths in 2021]")
                println("[3]-[Top 10 States with the highest number of births in 2021]")
                println("[4]-[Top 10 States with the highest population in 2021]")
                println("[5]-[Top 10 States with the highest number of births in 2020]")
                println("[6]-[Top 10 States with the highest population in 2020]")
                println("[7]-[Refresh Data]")
                
                option = scanner.nextInt()
                scanner.nextLine()

                
                if (option == 1){
                var result = hiveCtx.sql("SELECT Name, DEATHS2021 FROM popData WHERE State > 0 ORDER BY DEATHS2021 ASC LIMIT 10")
                println("The least Deaths in 2021:")
                result.show()
                } else if (option == 2){
                var result = hiveCtx.sql("SELECT Name, DEATHS2021 FROM popData WHERE State > 0 ORDER BY DEATHS2021 DESC LIMIT 10")
                println("The most Deaths in 2021:")
                result.show()
                } else if (option == 3){
                var result = hiveCtx.sql("SELECT Name, Births2021 FROM popData WHERE State > 0 ORDER BY Births2021 DESC LIMIT 10")
                println("The most Births in 2021:")
                result.show()
                } else if (option == 4){
                var result = hiveCtx.sql("SELECT Name, POPESTIMATE2021 FROM popData WHERE State > 0 ORDER BY POPESTIMATE2021 DESC LIMIT 10")
                println("The highest populatlion in 2021:")
                result.show()
                } else if (option == 5){
                var result = hiveCtx.sql("SELECT Name, Births2020 FROM popData WHERE State > 0 ORDER BY Births2020 DESC LIMIT 10")
                println("The most Births in 2020:")
                result.show()
                } else if (option == 6){
                var result = hiveCtx.sql("SELECT Name, POPESTIMATE2020 FROM popData WHERE State > 0 ORDER BY POPESTIMATE2020 DESC LIMIT 10")
                println("The highest populatlion in 2020:")
                result.show()
                } else if (option == 7){
                insertPopData(hiveCtx)
                }
           } while (option != 0)
        
       
    }
}
