package com.salesforce.citi.connector.execute;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

import org.apache.logging.log4j.util.SystemPropertiesPropertySource;
import org.apache.spark.sql.catalyst.expressions.UTCTimestamp;
import org.json.JSONArray;
import org.json.JSONObject;

public class DatabaseOperations {
	
	public static List<JSONObject> GetDataFromDatabase(String databasename, int index, String queryString) {
		Connection con = Utilities.getDataBaseConnection(databasename, index);
		List<JSONObject> dataJsonObjects = Utilities.getQueryResult(con, queryString);
		return dataJsonObjects;
	}

	
	public static String decryptData(String encryptedString) {
		System.out.println("encryptedString..."+encryptedString);
		
		String decryptedString=null;
		
		if (((JSONObject)Utilities.configdata.get("SYSTEM_PARAMETERS")).get("CREDENTIAL").equals("SYSTEM_ENVIRONMENT"))
			try {
				decryptedString=Utilities.executeShellCommand("sh","-c","echo "+encryptedString+"|base64 -d").replace("null","").replace("\n","");
				System.out.println("inside decryptedString..."+decryptedString);	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else if (((JSONObject)Utilities.configdata.get("SYSTEM_PARAMETERS")).get("CREDENTIAL").equals("FROM_DATABASE"))
			decryptedString=EncryptDecrypt.decryptPassword(encryptedString);
		
		 return decryptedString;
	}
	
	public static void SetDecryptedPasswordForDB(String resourcePropertyName,String DBType,int index) {
		 ReadResourceDataFromJar readFromJar=new ReadResourceDataFromJar();
		 String  resourceValueString =readFromJar.getApplicationResourcesJar().getProperty(resourcePropertyName);
		
		
		 String decryptedString=decryptData(resourceValueString);
		 System.out.print("decryptedString..."+decryptedString);
		 
		 JSONObject passwordJsonObject=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get(DBType)).get(index));
		 System.out.println(passwordJsonObject);
		 passwordJsonObject=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get(DBType)).get(index)).put("PASSWORD", decryptedString);
		 passwordJsonObject=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get(DBType)).get(index));
		 System.out.println(passwordJsonObject);	  
	}
	
	
	public static JSONObject getEncryptedPasswordFromOracleDBInJson() {
		
		String queryString=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get("ORACLE")).get(0)).get("QUERY_STRING").toString();
		if (((JSONObject)Utilities.configdata.get("SYSTEM_PARAMETERS")).get("CREDENTIAL").equals("FROM_DATABASE"))
			 SetDecryptedPasswordForDB("ORACLE_PASSWORD","ORACLE",0);
		 
		 List<JSONObject> valuesJsonObjects=DatabaseOperations.GetDataFromDatabase("ORACLE",0, queryString);
		 for (int i=0;i <valuesJsonObjects.size();i++)
			 System.out.println(valuesJsonObjects.get(i).toString(4));
		 	
		return valuesJsonObjects.get(0);
	}

	public static String getEncryptedPasswordFromOracleDBInString() {
		
		String queryString=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get("ORACLE")).get(0)).get("QUERY_STRING").toString();
		if (((JSONObject)Utilities.configdata.get("SYSTEM_PARAMETERS")).get("CREDENTIAL").equals("FROM_DATABASE"))
			 SetDecryptedPasswordForDB("ORACLE_PASSWORD","ORACLE",0);
		 
		 List<JSONObject> valuesJsonObjects=DatabaseOperations.GetDataFromDatabase("ORACLE",0, queryString);
		 
		 String decryptedStringString=valuesJsonObjects.get(0).get("PASSWORD")+"#$@"+valuesJsonObjects.get(0).get("CRED_PASS");
		
		return decryptedStringString;
	}

	public static String[] getEncryptedPasswordFromOracleDBInStringArray() {
		
		String queryString=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get("ORACLE")).get(0)).get("QUERY_STRING").toString();
		if (((JSONObject)Utilities.configdata.get("SYSTEM_PARAMETERS")).get("CREDENTIAL").equals("FROM_DATABASE"))
			 SetDecryptedPasswordForDB("ORACLE_PASSWORD","ORACLE",0);
		 
		 List<JSONObject> valuesJsonObjects=DatabaseOperations.GetDataFromDatabase("ORACLE",0, queryString);
		 
		 String decryptedStringString[]=null;
		 decryptedStringString[0]=valuesJsonObjects.get(0).get("PASSWORD").toString();
		 decryptedStringString[0]=valuesJsonObjects.get(0).get("CRED_PASS").toString();
		
		return decryptedStringString;
	}

	
	//################################## Main Function Area ##################################
	
	public static void main(String[] args) {
		 Utilities util=new Utilities();  ///Delete it
		 if (((JSONObject)Utilities.configdata.get("SYSTEM_PARAMETERS")).get("CREDENTIAL").equals("FROM_DATABASE"))
			 SetDecryptedPasswordForDB("ORACLE_PASSWORD","MYSQL",0);
		 
		 List<JSONObject> valuesJsonObjects=DatabaseOperations.GetDataFromDatabase("MYSQL",0, "Select ID,CountryCode from City Limit 1");
		 for (int i=0;i <valuesJsonObjects.size();i++)
			 System.out.println(valuesJsonObjects.get(i).toString(4));
		 System.out.println(valuesJsonObjects.get(0).get("ID")+"#$@"+valuesJsonObjects.get(0).get("CountryCode"));
		 
	}
}
