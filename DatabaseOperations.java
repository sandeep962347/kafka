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

	
	public static String DecryptData(String encryptedString) {
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
			try {
				decryptedString=Utilities.executeShellCommand(EncryptDecrypt.decryptPassword(encryptedString));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		 return decryptedString;
	}
	
	public static void SetDecryptedPasswordForDB(String resourcePropertyName,String DBType,int index) {
		 ReadResourceDataFromJar readFromJar=new ReadResourceDataFromJar();
		 String  resourceValueString =readFromJar.getApplicationResourcesJar().getProperty(resourcePropertyName);
		
		
		 String decryptedString=DecryptData(resourceValueString);
		 System.out.print("decryptedString..."+decryptedString);
		 
		 JSONObject passwordJsonObject=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get(DBType)).get(index));
		 System.out.println(passwordJsonObject);
		 passwordJsonObject=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get(DBType)).get(index)).put("PASSWORD", decryptedString);
		 passwordJsonObject=((JSONObject)((JSONArray)((JSONObject)(Utilities.configdata.get("DATABASE"))).get(DBType)).get(index));
		 System.out.println(passwordJsonObject);	  
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
