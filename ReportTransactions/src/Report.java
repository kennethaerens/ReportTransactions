import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
 
public class Report {
 
	private static Connection connection = null;
	private static PrintWriter writer = null;
	private static enum ReportType {TRANSACTIONS, VOLUMES};
	private static enum Dataset {MASSPOST, MAILID};
	private static Dataset datasetToQuery = null;
	
	public static void main(String[] argv) {		
		try { 
			Class.forName("oracle.jdbc.driver.OracleDriver"); 
		} catch (ClassNotFoundException e) { 
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return; 
		}
 
 		try { 
			connection = DriverManager.getConnection(
					"jdbc:oracle:thin:@//dbsmwp1.netpost:1521/SMWP1", "jcom_read", "jcom_read"); 
		} catch (SQLException e) { 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return; 
		}

 		/*createYearlyReport(2013);
 		createYearlyReport(2014);
 		createYearlyReport(2015); */
 		
 		/*createMonthlyReport(2013);
 		createMonthlyReport(2014);
 		createMonthlyReport(2015); */
 		
 		/*try {
			writer = new PrintWriter("C:/javaTest/outputWeeklyTransactions.txt");
			
			createWeeklyReport(2013);
	 		createWeeklyReport(2014);
	 		createWeeklyReport(2015);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
 		finally {
 			writer.close(); 			
 		}	*/
 		

 		FileOutputStream fout = null;
 		ObjectOutputStream oos = null;
 		try {
 	 		fout = new FileOutputStream("c:/javaTest/outputWeeklyTxsMailIDObjects.ser");
 			oos = new ObjectOutputStream(fout);
 			writer = new PrintWriter("C:/javaTest/outputWeeklyTxsMailID.txt");
 			
 			datasetToQuery = Dataset.MAILID;
 			
			createWeeklyReport(2013, ReportType.TRANSACTIONS, oos);
			createWeeklyReport(2014, ReportType.TRANSACTIONS, oos);
			createWeeklyReport(2015, ReportType.TRANSACTIONS, oos);
 		} catch (IOException e) {
			e.printStackTrace();
		}
 		finally {
 			writer.close(); 		

 			try {
				oos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
 		}
 		
	}
	
	private static void createWeeklyReport(int year, ReportType reportType, ObjectOutputStream oos) throws IOException {
		writer.println((reportType.equals(ReportType.TRANSACTIONS) ? "Transactions " : "Volumes ") + "for " + year);
				
		Calendar cal = Calendar.getInstance();
		cal.set(year, 0, 1, 0, 0);
		Date startDate = cal.getTime();
		
		List<DBRecordReport> lstResults = new ArrayList<DBRecordReport>();
		
		for (int week = 0; week < 52; week++) {
			System.out.println("Week: " + (week+1));
			writer.println("Week: " + (week+1));
			
			cal.setTime(startDate);				
			cal.add(Calendar.DATE, 7);
			if (week == 51)
				cal.add(Calendar.DATE, 1);
			
			Date endDate = cal.getTime();
			
			DateFormat jcomDateFormat = new SimpleDateFormat("yyyyMMdd");
			
			String strTable = (datasetToQuery.equals(Dataset.MASSPOST) ? "R029_MPA_AUDITRECORD" : 
				"M037_MID_AUDITRECORD");
			
			String sql = "select intchgindividual, sender, receiver, " + 
					(reportType.equals(ReportType.TRANSACTIONS) ? "count(auditno) " : "(sum(to_number(customkeyvalue2)) / 1024) ")
					+ "from JCOM_OWNER." + strTable + " "
					+ "where "
					+ "creationdate > '" + jcomDateFormat.format(startDate) + "000000' "
					+ "and creationdate < '" + jcomDateFormat.format(endDate) + "000000' "
					+ "group by INTCHGINDIVIDUAL, sender, receiver";
			
			writer.println(sql);
			
			PreparedStatement preStatement;
			try {
				preStatement = connection.prepareStatement(sql);
				
				System.out.println("running query...");
				
				ResultSet result = preStatement.executeQuery();			
				while(result.next()){
					DBRecordReport queryResult = new DBRecordReport();
					queryResult.setIntchgIndividual(result.getString("intchgindividual"));
					queryResult.setSender(result.getString("sender"));
					queryResult.setReceiver(result.getString("receiver"));
					queryResult.setCalculatedNumber(result.getString(4));
					
					lstResults.add(queryResult);
					oos.writeObject(queryResult);
					
					writer.println(
							queryResult.getIntchgIndividual() + "\t\t\t" + 
							queryResult.getSender() + "\t\t" + 
							queryResult.getReceiver() + "\t" + 
							(week+1) + "\t" + year + "\t" +
							queryResult.getCalculatedNumber());
					
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
				
			startDate = endDate;
		}
	}
	
	private static void createMonthlyReport(int year) {
		System.out.println("Transactions for " + year);
		
		for (int month = 0; month < 12; month++) {
			Calendar cal = Calendar.getInstance();
			cal.set(year, month, 1, 0, 0);
			Date startDate = cal.getTime();
			
			cal.add(Calendar.MONTH, 1);
			Date endDate = cal.getTime();
			
			DateFormat jcomDateFormat = new SimpleDateFormat("yyyyMMdd");
			
			String sql = "select intchgindividual, sender, receiver, count(auditno) "
					+ "from JCOM_OWNER.R029_MPA_AUDITRECORD "
					+ "where "
					+ "creationdate > '" + jcomDateFormat.format(startDate) + "000000' "
					+ "and creationdate < '" + jcomDateFormat.format(endDate) + "000000' "
					+ "group by INTCHGINDIVIDUAL, sender, receiver";
			
			System.out.println(sql);
			
			PreparedStatement preStatement;
			try {
				preStatement = connection.prepareStatement(sql);
				
				System.out.println("running query...");
				
				ResultSet result = preStatement.executeQuery();			
				while(result.next()){
					System.out.println(
							result.getString("intchgindividual") + "\t\t\t" + 
							result.getString("sender") + "\t\t" + 
							result.getString("receiver") + "\t" + 
							(month+1) + "\t" + year + "\t" +
							result.getString(4));
				}			
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void createYearlyReport(int year) {
		String sql = "select intchgindividual, sender, receiver, count(auditno) "
				+ "from JCOM_OWNER.R029_MPA_AUDITRECORD where creationdate > '" + year + "0101000000' "
				+ "and creationdate < '" + (year+1) + "0101000000' "
				+ "group by INTCHGINDIVIDUAL, sender, receiver";
		
		System.out.println(sql);
		
        PreparedStatement preStatement;
		try {
			preStatement = connection.prepareStatement(sql);
			
			System.out.println("running query...");
			
			ResultSet result = preStatement.executeQuery();			
			while(result.next()){
				System.out.println(
						result.getString("intchgindividual") + "\t\t\t" + 
						result.getString("sender") + "\t\t" + 
						result.getString("receiver") + "\t" + 
						result.getString(4) + "\t" +
						"year - " + year);
			}			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
 
}