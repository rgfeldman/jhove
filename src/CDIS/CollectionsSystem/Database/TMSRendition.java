/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.CollectionsSystem.Database;

import CDIS.CDIS;
import edu.si.data.DataProvider;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import edu.si.data.DataProvider;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TMSRendition {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    boolean isPrimary;
    int pixelH;
    int pixelW;
    int rank;
    int renditionId;
    String renditionNumber;
    String fileName;
    
    
    public String getFileName () {
        return this.fileName;
    }
    
    public boolean getIsPrimary() {
        return this.isPrimary;
    }
        
    public int getPixelH () {
        return this.pixelH;
    }
    
    public int getPixelW () {
        return this.pixelW;
    }
    
    public int getRank() {
        return this.rank;
    }
    
    public int getRenditionId () {
        return this.renditionId;
    }
    
    public String getRenditionNumber() {
        return this.renditionNumber;
    }
    

    
    
    private void setFileName (String fileName) {
        this.fileName = fileName;
    }
    
    private void setIsPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }
      
    private void setPixelH (int pixelH) {
        this.pixelH = pixelH;
    }
    
    private void setPixelW (int pixelW) {
        this.pixelW = pixelW;
    }
    
    private void setRank (int rank) {
        this.rank = rank;
    }
    
    private void setRenditionId (int renditionId) {
        this.renditionId = renditionId; 
    }
    
    public void setRenditionNumber (String renditionNumber) {
        this.renditionNumber = renditionNumber;
    }
    
    
    
    public void populateRenditionIdByRenditionNumber (TMSRendition tmsRendition, Connection tmsConn) {
        String sql = "Select RenditionID " +
                     "From MediaRenditions " +
                     "Where RenditionNumber = '" + tmsRendition.getRenditionNumber() + "'";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = tmsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    tmsRendition.setRenditionId(rs.getInt(1));
                }        
	}
            
	catch(SQLException sqlex) {
		sqlex.printStackTrace();
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        
    }
    
    public int populateTMSRenditionBarcode (String name, Connection tmsConn) {
    
        int objectID = 0;
        
        String sql = "Select ObjectID " +
              "from BCLabels bcl, " +
              "ObjComponents obc" +
              "where bcl.id = obc.component_id " +
              "and obc.TableID = 94 " +
              "and obc.LabelUUID = '" + name + "'";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = tmsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    objectID = rs.getInt(1);
                }        
	}
            
	catch(SQLException sqlex) {
		sqlex.printStackTrace();
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return objectID;
         
    }
    
    public void populateIsPrimary(Integer ObjectID, Connection tmsConn) {
        //Now that we have the rank, need to set the primary flag
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        int numRows = 0;
        
        if (this.getRank() == 1) {
            //Check to make sure if we dont have a rank 1 for this object already
            
            String sql = "SELECT count(*) from MediaXrefs " + 
                         "WHERE ID = " + ObjectID + 
                         " and PrimaryDisplay = 1";
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
             try {
		stmt = tmsConn.prepareStatement(sql);
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
     
    public String populateTMSRendition(String uoiid, TMSRendition tmsRendition, Connection damsConn) {
        
        String sql = "SELECT name, bitmap_height, bitmap_width from UOIS where UOI_ID = '" + uoiid + "'";
        String damsImageFileName = null;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    damsImageFileName = rs.getString(1).replace(".jpg", "");
                    damsImageFileName = damsImageFileName.replaceAll(".tif", "");
                    damsImageFileName = damsImageFileName.replaceAll(".TIF", ""); 

                    this.setPixelH(rs.getInt("bitmap_height"));
                    this.setPixelW(rs.getInt("bitmap_width"));
                }        
	}
            
	catch(SQLException sqlex) {
		sqlex.printStackTrace();
                return "";
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        // Get the rank from the last number after the '_' 
        String charRank = null;
        if (damsImageFileName.contains("_")) {
            int p = damsImageFileName.lastIndexOf("_");
            charRank = damsImageFileName.substring(p+1);
            
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
        
        
        logger.log(Level.FINER, "DAMS imageFileName: {0}", damsImageFileName);
        logger.log(Level.FINER, "Rank: {0}", this.getRank());
       
        return damsImageFileName;
    }   
  
}
