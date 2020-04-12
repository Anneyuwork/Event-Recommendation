package db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import db.DBConnection;
import entity.Item;
import entity.Item.ItemBuilder;
import external.TicketMasterClient;

public class MySQLConnection implements DBConnection {
	
	//1.create connection with mySQL
	private Connection conn; //avoid every time create a new object
	public MySQLConnection() {
		try {
	  		Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();//create a driver object -DriverManager
	  		conn = DriverManager.getConnection(MySQLDBUtil.URL);	//.getConnection	
	  	 } catch (Exception e) {
	  		 e.printStackTrace();
	  	 }
	 }

	@Override
	public void close() {
		 if (conn != null) {//if connection is created successfully--not null
	  		 try {
	  			 conn.close();
	  		 } catch (Exception e) {
	  			 e.printStackTrace();
	  		 }
	  	 }
		// TODO Auto-generated method stub
	}

	@Override
	public void setFavoriteItems(String userId, List<String> itemIds) {
		//add one line of data into history table 
		//one userId add a list of itemIds(more than one favorite items)
		if (conn == null) {
			System.err.println("DB connection failed");
			return;
		 }
		
		try {
			String sql = "INSERT IGNORE INTO history(user_id, item_id) VALUES (?, ?)";
			PreparedStatement ps = conn.prepareStatement(sql);//create a ps object, till now know where to add
			//following is what to add
			ps.setString(1, userId);
			for (String itemId : itemIds) {//iterate itemIds
				 ps.setString(2, itemId);//in the second for loop, column 1 is not change, but column 2 iterated and changed
				 ps.execute();
			}
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	}

	@Override
	public void unsetFavoriteItems(String userId, List<String> itemIds) {
		if (conn == null) {
			 System.err.println("DB connection failed");
			 return;
		   }
		//only different is remove
		 try {
			 String sql = "DELETE FROM history WHERE user_id = ? AND item_id = ?";
			 PreparedStatement ps = conn.prepareStatement(sql);
			 ps.setString(1, userId);
			 for (String itemId : itemIds) {
				 ps.setString(2, itemId);
				 ps.execute();
			 }
		
		  } catch (Exception e) {
			  e.printStackTrace();
		  }

	}

	@Override
	//based on userId return intemId
	public Set<String> getFavoriteItemIds(String userId) {
		if (conn == null) {
			return new HashSet<>();
		}
		
		Set<String> favoriteItems = new HashSet<>();
		
		try {
			//history table have itemId and userId, we only care itemId and return itemId
			String sql = "SELECT  item_id FROM history WHERE user_id = ?";//
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, userId);
			//one user can have more than one itemId, 
			//but one user cannot like one item for more than one time, since we unset favorite
			ResultSet rs = stmt.executeQuery();//one row is one element in set 
			
			while (rs.next()) {
				String itemId = rs.getString("item_id");//one column, only need itemId
				favoriteItems.add(itemId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return favoriteItems;//this user likes what itemId

	}

	@Override
	public Set<Item> getFavoriteItems(String userId) {
		if (conn == null) {
			return new HashSet<>();
		}
		
		Set<Item> favoriteItems = new HashSet<>();
		Set<String> itemIds = getFavoriteItemIds(userId);//1.based on userId to get itemId, call getFavoriteItemIds
		//2.know itemId, need to find the item from item table 
		try {
			String sql = "SELECT * FROM items WHERE item_id = ?";//* return all value of this column
			PreparedStatement stmt = conn.prepareStatement(sql);
			for (String itemId : itemIds) {
				stmt.setString(1, itemId);
				
				ResultSet rs = stmt.executeQuery();
				
				ItemBuilder builder = new ItemBuilder();
				
				while (rs.next()) {
					builder.setItemId(rs.getString("item_id"));
					builder.setName(rs.getString("name"));
					builder.setAddress(rs.getString("address"));
					builder.setImageUrl(rs.getString("image_url"));
					builder.setUrl(rs.getString("url"));
					builder.setCategories(getCategories(itemId));//call function getCategories to get Categories based on ItemId
					builder.setDistance(rs.getDouble("distance"));
					builder.setRating(rs.getDouble("rating"));
					
					favoriteItems.add(builder.build());//add back into favoriteItems
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return favoriteItems;
	}

	@Override
	public Set<String> getCategories(String itemId) {
		if (conn == null) {
			return null;
		}
		Set<String> categories = new HashSet<>();
		try {
			//category table have two rows: category & itemId, only care category
			String sql = "SELECT category from categories WHERE item_id = ? ";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setString(1, itemId);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				String category = rs.getString("category");//get category from all the rows
				categories.add(category);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return categories;

	}

	@Override
	//2.same as servlet, create items list, iterate the item and save in the database, and return the list
	public List<Item> searchItems(double lat, double lon, String term) {	
		  TicketMasterClient ticketMasterClient = new TicketMasterClient();
	      List<Item> items = ticketMasterClient.search(lat, lon, term);//got list of item
	
	      for(Item item : items) {//iterate the item, and save in the database 
	    	  saveItem(item);
	      }
	
	      return items;

	}

	@Override
	//3.save data into sql item table and categories table
	public void saveItem(Item item) {
        if (conn == null) {
			System.err.println("DB connection failed");
			return;
		 }
		
		try {
			//3.1 add data into item table
			String sql = "INSERT IGNORE INTO items VALUES (?, ?, ?, ?, ?, ?, ?)";//7? means 7 fields
			//without IGNORE, if we miss one keyword, the entire insert will fail
			PreparedStatement ps = conn.prepareStatement(sql);// PreparedStatement is helping us to setup value for each column for sql statement
			ps.setString(1, item.getItemId());//item.getItemId() is a String, than add into the ps
			ps.setString(2, item.getName());
			ps.setDouble(3, item.getRating());
			ps.setString(4, item.getAddress());
			ps.setString(5, item.getImageUrl());
			ps.setString(6, item.getUrl());
			ps.setDouble(7, item.getDistance());
			ps.execute();// till now, we add one row of data in the item table, which have values in this 7 columns
			
			//3.2 add data into categories table
			sql = "INSERT IGNORE INTO categories VALUES(?, ?)";
			ps = conn.prepareStatement(sql);
			                // for example:itemed 123
			ps.setString(1, item.getItemId());
			                // for example:pop, music
			for(String category : item.getCategories()) {//categories is a set in item object, need to iterate the set
				 ps.setString(2, category);
				 ps.execute();//the example add 2 categories into the categories table
			}
		    // this example have 1 item id for 2 categories, better to have item id outside of the for loop
			
		} catch (Exception e) {//e is the error
			e.printStackTrace();
}
	}

	@Override
	public String getFullname(String userId) {
		if (conn == null) {
			return "";
		}		
		String name = "";
		try {
			String sql = "SELECT first_name, last_name FROM users WHERE user_id = ? ";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setString(1, userId);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				name = rs.getString("first_name") + " " + rs.getString("last_name");
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return name;

	}

	@Override
	public boolean verifyLogin(String userId, String password) {
		if (conn == null) {
			return false;
		}
		try {
			String sql = "SELECT user_id FROM users WHERE user_id = ? AND password = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, userId);
			stmt.setString(2, password);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return false;
	}
	
	@Override
	public boolean registerUser(String userId, String password, String firstname, String lastname) {
		if (conn == null) {
			System.err.println("DB connection failed");
			return false;
		}

		try {
			String sql = "INSERT IGNORE INTO users VALUES (?, ?, ?, ?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, userId);
			ps.setString(2, password);
			ps.setString(3, firstname);
			ps.setString(4, lastname);
			
			return ps.executeUpdate() == 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;	
	}

}
