/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.utilties.DataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MediaXrefs {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    int primary;
    int rank;
    String charRank;
        
    public int getPrimary() {
        return this.primary;
    }
    
    public int getRank() {
        return this.rank;
    }
    
    public String getcharRank() {
        return this.charRank;
    }
    
    private void setPrimary(int primary) {
        this.primary = primary;
    }
      
    private void setRank (int rank) {
        this.rank = rank;
    }
        
     /*  Method :        populateIsPrimary
        Arguments:      
        Description:    calculates and populates the isPrimary member variable
        RFeldman 3/2015
    */
    public void populateIsPrimary(Integer ObjectID, Connection cisConn) {
        //Now that we have the rank, need to set the primary flag
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        // set primary to 0 for default
        this.primary = 0; 
        int numRows = 0;
        
        if (this.getRank() == 1) {
            //Check to make sure if we dont have a rank 1 for this object already
            
            String sql = "SELECT count(*) from MediaXrefs" + 
                         " WHERE ID = " + ObjectID + 
                         " and PrimaryDisplay = 1" +
                         " and TableID = '108'";
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
             try {
		stmt = cisConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                
                if (rs.next()) {
                    numRows = rs.getInt(1);
                }
                
             }
             catch(SQLException sqlex) {
		sqlex.printStackTrace();
            }
            finally {
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            }
            
            // Set the primary to 1 if there were no prior existing primaries found
            if (numRows == 0) {
                logger.log(Level.FINEST, "Primary set to 1");
                this.setPrimary(1);
            }
        }    
        
    }
    
    
    public void calculateRank(String extensionlessFileName) {
        
        
        // Get the rank from the last number after the '_' 
        String charRank = null;
        if (extensionlessFileName.contains("-r")) {
            int startPos = extensionlessFileName.lastIndexOf("-r") + 2;
            
            if (extensionlessFileName.substring(startPos).contains("-") ) {
                charRank = extensionlessFileName.substring(startPos,extensionlessFileName.lastIndexOf("-"));
            }
            else {
                charRank = extensionlessFileName.substring(startPos);
            }
        }
        else if (extensionlessFileName.contains("_")) {
            int pos = extensionlessFileName.lastIndexOf("_");
            charRank = extensionlessFileName.substring(pos+1);
        }
        
        logger.log(Level.FINER, "Char Rank: {0}", charRank);
        
        if (charRank != null) {
            try {
                this.setRank(Integer.parseInt(charRank));
                
            } catch (Exception e ) {
                //rank is not an int...set rank to one as default, and set to primary
                this.setRank(1); 
            }
        }
        
        else {
            // dams Filename doesnt contain underscore, set the rank to 1
            this.setRank(1);     
        }
        
        logger.log(Level.FINER, "DAMS imageFileName: {0}", extensionlessFileName);
        logger.log(Level.FINER, "Rank: {0}", this.getRank());
    
    }
    
    public boolean insertNewRecord(Connection cisConn, Integer mediaMasterId, Integer objectId) {
     
        Boolean inserted;
        
        String sql = "insert into MediaXrefs" +
                        " (MediaMasterID, " +
                        "ID, " +
                        "TableID, " + 
                        "LoginID, " +
                        "EnteredDate, " +
                        "Rank, " +
                        "PrimaryDisplay)" +
                    "values(" +
                        mediaMasterId + ", " +
                        objectId + ", " +  
                        "108, " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        this.rank + ", " + 
                        this.primary + ")" ;
        
        logger.log(Level.FINER, "SQL: {0}", sql);
   
        inserted = DataProvider.executeInsert(cisConn, sql);     
                
        return inserted;
                
    }
}
