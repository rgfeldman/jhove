/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.utilties.Transform;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;


        
public class TMSObject {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer objectID;
    String objectNumber;
    
    
    public int getObjectID () {
        return this.objectID;
    }
    
    public String getObjectNumber () {
        return this.objectNumber;
    }
    
   
    public void setObjectID (int objectID) {
        this.objectID = objectID;
    }
    
    private void setObjectNumber (String objectNumber) {
        this.objectNumber = objectNumber;
    }
        
    
    /*  Method :        populateObjectFromImageName
        Arguments:      
        Returns:      
        Description:    Finds the objectID and populates object member variables based on the DAMS image name 
        RFeldman 2/2015
    */
    public boolean populateObjectFromImageName (String damsImageFileName, CDIS cdis_new, Connection tmsConn){

        // populate ObjectNumber using various formats specified in the config file
        String damsDelimiter = cdis_new.properties.getProperty("damsDelimiter");
        String tmsDelimiter = cdis_new.properties.getProperty("tmsDelimiter");
        int imageObjectTrunc = Integer.parseInt(cdis_new.properties.getProperty("imageObjectTrunc"));
        
        String tmpObjectNumber = null;
        
        Transform transform = new Transform();
        
        logger.log(Level.FINER,"Need to find Object for filename...before reformat " + damsImageFileName);
        
        //Find the objectNumber based on the Rendition number
        tmpObjectNumber = transform.transform(damsImageFileName,damsDelimiter,tmsDelimiter,0,imageObjectTrunc);
        
        if (damsDelimiter.equals("ACM")) {
            if (damsImageFileName.startsWith("ACM-acmobj-")) {
                tmpObjectNumber = damsImageFileName.replaceAll("ACM-acmobj-", "");
            }
            else {
                tmpObjectNumber = damsImageFileName.replaceAll("acmobj-", "");  
            }
            if (tmpObjectNumber.contains("-")) {
                tmpObjectNumber = tmpObjectNumber.substring(0, tmpObjectNumber.indexOf("-"));
            }
        }
        else {
            tmpObjectNumber = damsImageFileName;
        }
        
        if (tmsDelimiter.equals("dotsEveryFour")) {
            setObjectNumber(tmpObjectNumber.substring(0, 4) + "." + tmpObjectNumber.substring(4, 8) + "." +  tmpObjectNumber.substring(8));
        }
        else {
            setObjectNumber(tmpObjectNumber);
        }
        
        logger.log(Level.FINER,"NEW TMS ObjectNumber: " + getObjectNumber());
    
        // Obtain the ObjectID based on the ObjectName that was determined above. 
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String sql =    "select ObjectID " +
                        "from Objects " +
                        "where ObjectNumber = '" + getObjectNumber() + "'";
                    
                logger.log(Level.FINEST,"SQL! " + sql);
        
        try {
            stmt = tmsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                setObjectID(rs.getInt(1));
            }
            else {
                return false;
            }
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    /*  Method :        populateObjectIDByRenditionId
        Arguments:      
        Returns:      
        Description:    Finds, and sets the objectID based on the RenditionID 
        RFeldman 2/2015
    */
    public void populateObjectIDByRenditionId (Integer renditionId, Connection tmsConn) {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String sql =    "select a.ObjectID " +
                        "from Objects a, " +
                        "MediaXrefs b, " +
                        "MediaMaster c, " +
                        "MediaRenditions d " +
                        "where a.ObjectID = b.ID " +
                        "and b.MediaMasterID = c.MediaMasterID " +
                        "and b.TableID = 108 " +
                        "and c.MediaMasterID = d.MediaMasterID " +
                        "and d.RenditionID = " + renditionId;
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try {
            stmt = tmsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                logger.log(Level.FINER,"Located ObjectID: " + rs.getInt(1) + " For RenditionID: " + renditionId);
                setObjectID(rs.getInt(1));
            }
        } catch (Exception e) {
                e.printStackTrace();
        
        }finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
    }
    
}

