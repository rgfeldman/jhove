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
    int renditionID;
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
    
    public int getRenditionID () {
        return this.renditionID;
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
    
    private void setRenditionID (int renditionID) {
        this.renditionID = renditionID;
    }
    
    public void setRenditionNumber (String renditionNumber) {
        this.renditionNumber = renditionNumber;
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
     
    public String populateTMSRendition(String uoiid, TMSRendition tmsRendition, Connection damsConn) {
        
        String sql = "SELECT name, bitmap_height, bitmap_width from UOIS where UOI_ID = '" + uoiid + "'";
        String damsRenditionName = null;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    damsRenditionName = rs.getString(1).replace(".jpg", "");
                    damsRenditionName = damsRenditionName.replace(".tif", "");

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
        
        // Get the rank from the last number after the '-' 
        //first populate the default rank as 1
        this.setRank(1);
        
        String[] tmpString = damsRenditionName.split("_");   
        String charRank = tmpString[tmpString.length-1];
        
        logger.log(Level.FINER, "new RenditionNumber: {0}", tmsRendition.getRenditionNumber());
        logger.log(Level.FINER, "Rank: {0}", charRank);
        
        this.setRank(Integer.parseInt(charRank));      
        
        // the isPrimary flag is based on the rank
        if ( tmsRendition.getRank() == 1 ) {
            this.setIsPrimary(true);
        }
        else {
            this.setIsPrimary(false);
        }     
       
        return damsRenditionName;
    }   
  
}
