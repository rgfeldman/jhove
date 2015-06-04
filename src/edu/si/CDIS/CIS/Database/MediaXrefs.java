/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
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
    
    boolean isPrimary;
    int rank;
    String charRank;
        
    public boolean getIsPrimary() {
        return this.isPrimary;
    }
    
    public int getRank() {
        return this.rank;
    }
    
    public String getcharRank() {
        return this.charRank;
    }
    
    private void setIsPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
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
            
            if (numRows == 0) {
                this.setIsPrimary(true);
            }
            else {
                this.setIsPrimary(false);
            }
        }
        else { 
            this.setIsPrimary(false);
        }
        
    }
    
    
    public void calculateRank(String imageName) {
        
        String extensionlessImageName = imageName.substring(0, imageName.lastIndexOf("."));
        
        // Get the rank from the last number after the '_' 
        String charRank = null;
        if (extensionlessImageName.contains("-r")) {
            int startPos = extensionlessImageName.lastIndexOf("-r") + 2;
            
            if (extensionlessImageName.substring(startPos).contains("-") ) {
                charRank = extensionlessImageName.substring(startPos,extensionlessImageName.lastIndexOf("-"));
            }
            else {
                charRank = extensionlessImageName.substring(startPos);
            }
        }
        else if (extensionlessImageName.contains("_")) {
            int pos = extensionlessImageName.lastIndexOf("_");
            charRank = extensionlessImageName.substring(pos+1);
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
        
        logger.log(Level.FINER, "DAMS imageFileName: {0}", extensionlessImageName);
        logger.log(Level.FINER, "Rank: {0}", this.getRank());
    
    }
    
    public void insertNewRecord() {
        
    }
}
