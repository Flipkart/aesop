package com.flipkart.aesop.relay.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlIngestor {

	private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	private Connection conn = null;
	private final String DB_URL = "jdbc:mysql://localhost/or_test";
	private final String USERNAME="root";
	private final String PASSWORD ="";
	private final int NO_OPS = 10;
	private final String ID_FILE = "id.txt";


	private void executeTests() throws Exception
	{
		Statement stmt = null;
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL,USERNAME,PASSWORD);
			int id = readId();
			insertTest(stmt,id);
			System.out.println("******************Insertion Complete *************");
			updateTest(stmt,id);
			System.out.println("******************Update Complete *************");
			deleteTest(stmt, id);
			System.out.println("******************Delete Complete *************");
		}finally{
			if(conn!=null)
				conn.close();
		}

	}

	private int readId() {
		InputStream is = getClass().getClassLoader().getResourceAsStream(ID_FILE);
		BufferedReader reader = null;
		try
		{
			reader = new BufferedReader(new InputStreamReader(is));
			return Integer.parseInt(reader.readLine());
		}catch (Exception e) {
			return 1;
		}finally{
			if(reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	private void writeId(int id) {
		PrintWriter writer = null;
		try
		{
			writer = new PrintWriter(new File(getClass().getClassLoader().getResource(ID_FILE).getPath()));
			writer.write(id+"");
			writer.flush();
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(writer != null)
				writer.close();
		}
	}


	private void insertTest(Statement stmt , int id) throws SQLException {
		String sql = "insert into person values(?,?,?,?,?)";
		PreparedStatement pStmt = null;
		try
		{
			pStmt = conn.prepareStatement(sql);
			for(int i = id ; i < id+NO_OPS ; i++)
			{
				pStmt.setInt(1, i);
				pStmt.setString(2, "Mr.Aesopfirst" + i);
				pStmt.setString(3, "Mr.Aesoplast" + i);
				pStmt.setDate(4, new Date(System.currentTimeMillis()));
				pStmt.setString(5, "false");
				pStmt.addBatch();
			}
			pStmt.executeBatch();
			writeId(id+NO_OPS);
		}finally
		{
			if(pStmt != null && !pStmt.isClosed())
				pStmt.close();
		}
	}

	private void updateTest(Statement stmt, int id) throws SQLException {
		String sql = "update person set first_name = ? where id = ?";
		PreparedStatement pStmt = null;
		try
		{
			pStmt = conn.prepareStatement(sql);
			for(int i = id ; i < id+NO_OPS ; i++)
			{
				pStmt.setString(1, "Mr.Aesopfirst"+i+i);
				pStmt.setInt(2,i);
				pStmt.addBatch();
			}
			pStmt.executeBatch();
		}finally
		{
			if(pStmt != null && !pStmt.isClosed())
				pStmt.close();
		}
	}
	
	private void rollbackTest(Statement stmt, int id) throws SQLException {
		String sql = "update person set first_name = ? where id = ?";
		PreparedStatement pStmt = null;
		try
		{
			conn.setAutoCommit(false);
			pStmt = conn.prepareStatement(sql);
			for(int i = id ; i < id+NO_OPS ; i++)
			{
				pStmt.setString(1, "Mr.Aesopfirst"+i+i);
				pStmt.setInt(2,  i);
				pStmt.addBatch();
			}
			pStmt.executeBatch();
		}finally
		{
			if(pStmt != null && !pStmt.isClosed())
				pStmt.close();
			conn.rollback();
		}
	}
	
	private void deleteTest(Statement stmt, int id) throws SQLException {
		String sql = "delete from person where id = ?";
		PreparedStatement pStmt = null;
		try
		{
			pStmt = conn.prepareStatement(sql);
			for(int i = 0 ; i < (NO_OPS)/2 ; i++)
			{
				pStmt.setInt(1,  id+i);
				pStmt.addBatch();
			}
			pStmt.executeBatch();
		}finally
		{
			if(pStmt != null && !pStmt.isClosed())
				pStmt.close();
		}
	}


	public static void main(String[] args) throws Exception {
		MysqlIngestor ingest = new MysqlIngestor();
		ingest.executeTests();
	}

}
