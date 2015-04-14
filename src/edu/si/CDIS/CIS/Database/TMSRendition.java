/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.utilties.DataProvider;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import edu.si.CDIS.utilties.DataProvider;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TMSRendition {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    boolean isPrimary;
    int pixelH;
    int pixelW;
    int rank;
    String charRank;
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
    
    public String getcharRank() {
        return this.charRank;
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
        String sql = "Select max (RenditionID) " +
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
    
     /*  Method :       getObjectIDFromBarcode
        Arguments:      
        Description:    obtains the objectID from a barcoded DAMS image
        RFeldman 3/2015
    */
    public int getObjectIDFromBarcode (String barcode, Connection tmsConn) {
    
        int objectID = 0;
        
        //Strip all characters in the barcode after the underscore to look up the label
        if (barcode.contains("_")) {
           barcode = barcode.substring(0,barcode.indexOf("_")); 
        }
        
        String sql = "Select ObjectID " +
              "from BCLabels bcl, " +
              "ObjComponents obc " +
              "where bcl.id = obc.Componentid " +
              "and bcl.TableID = 94 " +
              "and bcl.LabelUUID = '" + barcode + "'";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = tmsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    objectID = rs.getInt(1);
                }        
                else {
                    logger.log(Level.FINEST, "Unable to find Object from Barcode:{0}", barcode);
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
    
    /*  Method :        populateIsPrimary
        Arguments:      
        Description:    calculates and populates the isPrimary member variable
        RFeldman 3/2015
    */
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
     
    /*  Method :        populateRenditionFromDamsInfo
        Arguments:      
        Description:    determines and populates the tmsRendition member variables from information in DAMS
        RFeldman 3/2015
    */
    public String populateRenditionFromDamsInfo(String uoiid, TMSRendition tmsRendition, Connection damsConn) {
        
        String sql = "SELECT name, bitmap_height, bitmap_width from UOIS where UOI_ID = '" + uoiid + "'";
        String extensionlessImageName = null;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    //Drop the extension off of the image name
                    extensionlessImageName = rs.getString(1).substring(0, rs.getString(1).lastIndexOf("."));
                   
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
       
        return extensionlessImageName;
    }   
  
}
