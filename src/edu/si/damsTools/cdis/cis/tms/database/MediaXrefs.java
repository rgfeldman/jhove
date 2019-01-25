/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.database;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.utilities.StringUtils;
import edu.si.damsTools.utilities.XmlUtils;
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
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer mediaMasterId;
    private Integer objectId;
    private Integer primary;
    private Integer rank;
    private String charRank;
    private String tableId;
        
    public Integer getMediaMasterId() {
        return this.mediaMasterId;
    }
    
    public Integer getObjectId() {
        return this.objectId;
    }
            
    public Integer getPrimary() {
        return this.primary;
    }
    
    public Integer getRank() {
        return this.rank;
    }
    
    public String getcharRank() {
        return this.charRank;
    }
    
    public String getTableId() {
        return this.tableId;
    }
    
    public void setMediaMasterId(Integer mediaMasterId) {
        this.mediaMasterId = mediaMasterId;
    }
    public void setObjectId(Integer objectId) {
        this.objectId = objectId;
    }
            
    private void setPrimary(Integer primary) {
        this.primary = primary;
    }
      
    private void setRank (Integer rank) {
        this.rank = rank;
    }
    
    public void setTableId (String tableId) {
        this.tableId = tableId;
    }
        
     /*  Method :        populateIsPrimary
        Arguments:      
        Description:    calculates and populates the isPrimary member variable
        RFeldman 3/2015
    */
    public void populateIsPrimary() {
        //Now that we have the rank, need to set the primary flag
        // set primary to 0 for default
        this.primary = 0; 
        int numRows = 0;
        
        if (this.getRank() == 1 || XmlUtils.getConfigValue("rankOverride") != null) {
            //Check to make sure if we dont have a rank 1 for this object already
            
            String sql = "SELECT count(*) from MediaXrefs" + 
                         " WHERE ID = " + getObjectId() + 
                         " and PrimaryDisplay = 1" +
                         " and TableID = '" + this.tableId + "'";
            
            logger.log(Level.FINEST, "SQL: {0}", sql);  
            try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                  ResultSet rs = pStmt.executeQuery() ) {
		
                if (rs.next()) {
                    numRows = rs.getInt(1);
                }  
             }
             catch(SQLException sqlex) {
		sqlex.printStackTrace();
            }
            
            // Set the primary to 1 if there were no prior existing primaries found
            if (numRows == 0) {
                logger.log(Level.FINEST, "Primary set to 1");
                this.setPrimary(1);
            }
        }    
    }
    
    
    public void calculateRankFromDams(DamsRecord damsRecord) {
        
        if (XmlUtils.getConfigValue("rankOverride") != null) {
            this.setRank(Integer.parseInt(XmlUtils.getConfigValue("rankOverride")));
            return;
        }
        
        String extensionlessName = StringUtils.getExtensionlessFileName(damsRecord.getUois().getName());
        // Get the rank from the last number after the '_' 
        String charRank = null;
        if (extensionlessName.contains("-r")) {
            int startPos = extensionlessName.lastIndexOf("-r") + 2;
            
            if (extensionlessName.substring(startPos).contains("-") ) {
                charRank = extensionlessName.substring(startPos,extensionlessName.lastIndexOf('-'));
            }
            else {
                charRank = extensionlessName.substring(startPos);
            }
        }
        else if (extensionlessName.contains("_")) {
            int pos = extensionlessName.lastIndexOf('_');
            charRank = extensionlessName.substring(pos+1);
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
        
        logger.log(Level.FINER, "DAMS imageFileName: {0}", damsRecord.getUois().getName());
        logger.log(Level.FINER, "Rank: {0}", this.getRank());
    
    }
    
    public boolean insertNewRecord() {
     
        int insertCount;
        ResultSet rs = null;
        
        String sql = "insert into MediaXrefs (" +
                        "MediaMasterID, " +
                        "ID, " +
                        "TableID, " + 
                        "LoginID, " +
                        "EnteredDate, " +
                        "Rank, " +
                        "PrimaryDisplay)" +
                    "values(" +
                        getMediaMasterId() + ", " +
                        getObjectId() + ", " +  
                        getTableId() + ", " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        this.rank + ", " + 
                        this.primary + ")" ;
        
        logger.log(Level.FINER, "SQL: {0}", sql);
   
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql)) {
            
            insertCount = pStmt.executeUpdate();
             
            if (insertCount != 1) {
                throw new Exception();
            }
        
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to insert into mediaXrefs table", e );
            return false;    
        }
        
        return true; 
    }
}
