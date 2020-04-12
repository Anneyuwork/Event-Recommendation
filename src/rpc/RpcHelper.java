package rpc;

import java.io.BufferedReader;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.JSONArray;
import org.json.JSONObject;

public class RpcHelper {
	// Writes a JSONArray to http response.
	public static void writeJsonArray(HttpServletResponse response, JSONArray array) throws IOException{
		response.setContentType("application/json");
		response.getWriter().print(array);
	}

              // Writes a JSONObject to http response.
	public static void writeJsonObject(HttpServletResponse response, JSONObject obj) throws IOException {		
		response.setContentType("application/json");
		response.getWriter().print(obj);
	}
	//help with the unsetFavoriteItem 
	//read based on request and return the JSONObject
	public static JSONObject readJSONObject(HttpServletRequest request) {
	  	StringBuilder sBuilder = new StringBuilder();
	  	try (BufferedReader reader = request.getReader()) {
		  	String line = null;
		  	while((line = reader.readLine()) != null) {
		  		sBuilder.append(line);//read every line, and build the entire information
		  	}
		  	return new JSONObject(sBuilder.toString());//get the information
	  		
	  	} catch (Exception e) {
	  		e.printStackTrace();
	  	}
	  
	  	return new JSONObject();
	 }
}

