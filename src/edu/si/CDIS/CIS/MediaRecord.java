/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.utilties.DataProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import org.apache.commons.io.IOUtils;
import java.sql.CallableStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import edu.si.CDIS.CIS.Database.TMSRendition;
import edu.si.CDIS.CIS.Database.TMSObject;
import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class MediaRecord {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection damsConn;
    Connection cisConn;
    
    
    private boolean formatNewRenditionNumber (CDIS cdis_new, String damsImageFileName, TMSRendition tmsRendition) {
        
        logger.log(Level.FINER, "Dams Image fileName before formatting: {0}", damsImageFileName);
        String tmpRenditionNumber = null;
        
        String tmsDelimiter = cdis_new.properties.getProperty("tmsDelimiter");
        String damsDelimiter = cdis_new.properties.getProperty("damsDelimiter");
        
        // If the delimeter is different from the image to the renditionNumber, we need to put the appropriate delimeter in the newly created name
        if (tmsDelimiter.equals("ACM")) {
            if (damsImageFileName.startsWith("ACM-")) {
                tmpRenditionNumber = damsImageFileName.replaceAll("ACM-", "");
            }
            else {
                tmpRenditionNumber = damsImageFileName;
            }
            if (tmpRenditionNumber.substring(7).contains("-")) {
                
                // Chop off everything in the body (after acmboj-) following the last dash....except in the cases where it has the rank number 
                if (! tmpRenditionNumber.substring(tmpRenditionNumber.lastIndexOf("-")).startsWith("r")) { 
                    // Safe to chop off the end, the last dash does not contain a -r 
                    tmpRenditionNumber = tmpRenditionNumber.substring(0, tmpRenditionNumber.lastIndexOf("-"));
                }
            }
            tmsRendition.setRenditionNumber(tmpRenditionNumber);
        }
        else if (! tmsDelimiter.equals (damsDelimiter) ) {
            tmsRendition.setRenditionNumber (damsImageFileName.replaceAll(damsDelimiter, tmsDelimiter));      
        }
        else if (tmsDelimiter.equals (damsDelimiter)) {
            tmsRendition.setRenditionNumber(damsImageFileName);
        }
        else {
            logger.log(Level.FINER, "unable to create Rendition number, Invalid name formatting option: {0}", cdis_new.properties.getProperty("newRenditionNameFormat"));
            return false;
        }
        
        logger.log(Level.FINER, "Formatted name: {0}", tmsRendition.getRenditionNumber());
        
        return true;
        
    }
    
    public boolean create (CDIS cdis_new, SiAssetMetaData siAsst, TMSRendition tmsRendition, TMSObject tmsObject) {
 
        this.cisConn = cdis_new.cisConn;    
        this.damsConn = cdis_new.damsConn;
        
        boolean objectPopulated = false;
        boolean MediaCreated;
       
        //Get the rendition name, and the dimensions
        // if the barcode is set, use the name to get the barcode info,
        //else use the name to get rendition name with the rendition   
//        String damsImageFileName = tmsRendition.populateRenditionFromDamsInfo(siAsst.getUoiid(), tmsRendition, damsConn);
        String imageFileName = tmsRendition.populateRenditionFromDamsInfo(siAsst.getUoiid(), tmsRendition, damsConn);
        String damsImageFileName = imageFileName;
        String fileType = "";
        int len = imageFileName.lastIndexOf(".");
        // Check if file name have file extension?
        if (len > 0) {
        	damsImageFileName = imageFileName.substring(0, imageFileName.lastIndexOf("."));
        	fileType = imageFileName.substring(imageFileName.lastIndexOf(".")+1, imageFileName.length());
        }
        
        // If we are dealing with barcode logic, the name of the rendition that we are mapping to in TMS,
        // and the objectID is populated by an alternate method
        if (cdis_new.properties.getProperty("mapFileNameToBarcode").equals("true")) {
            objectPopulated = tmsObject.mapFileNameToBarcode(damsImageFileName, cisConn);
                
            if (objectPopulated) {
                
                // For NASM, we have to append the timestamp to the renditionName only on barcoded objects for uniqueness
                if (cdis_new.properties.getProperty("appendTimeToNumber").equals("true"))  {
                    DateFormat df = new SimpleDateFormat("kkmmss");
                    tmsRendition.setRenditionNumber(tmsObject.getObjectID() + "_" + String.format("%03d", tmsRendition.getRank()) + "_" + df.format(new Date()));
                }
                else {
                    // For barcode objects, the renditionNumber is the objectID plus the rank
                    tmsRendition.setRenditionNumber(tmsObject.getObjectID() + "_" + tmsRendition.getRank() );
                }
            }
        }
        
        if (! objectPopulated) {
            if (cdis_new.properties.getProperty("mapFileNameToObjectNumber").equals("true")) {
            
                objectPopulated = tmsObject.mapFileNameToObjectNumber(damsImageFileName, cdis_new);
                if (objectPopulated) {
                    formatNewRenditionNumber (cdis_new, damsImageFileName, tmsRendition);
                }
 
            }
        }
        
        if (! objectPopulated) {
            if (cdis_new.properties.getProperty("mapFileNameToObjectID").equals("true")) {
               
                objectPopulated = tmsObject.mapFileNameToObjectID(damsImageFileName, cisConn);
                if (objectPopulated) {
                    tmsRendition.setRenditionNumber(damsImageFileName); 
                }
                
            }
        }
        
        if (! objectPopulated) {
                // we were unable to populate the object, return with a failure indicator

                //Set the RenditionNumber as the filename for reporting purposes
                tmsRendition.setRenditionNumber(damsImageFileName);
                logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain object Data");
                return false;
        }
        
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
        String renditionDate = df1.format(cal.getTime());
        
        // Set the primaryRenditionFlag
        tmsRendition.populateIsPrimary(tmsObject.getObjectID(), cisConn);
        
        logger.log(Level.FINER, "about to create TMS media Record:");
        logger.log(Level.FINER, "ObjectID: " + tmsObject.getObjectID());
        logger.log(Level.FINER, "RenditionNumber: {0}", tmsRendition.getRenditionNumber());
        logger.log(Level.FINER, "Rank: " + tmsRendition.getRank());
        logger.log(Level.FINER, "PixelH: " + tmsRendition.getPixelH());
        logger.log(Level.FINER, "PixelW: " + tmsRendition.getPixelW());
        logger.log(Level.FINER, "IsPrimary: " + tmsRendition.getIsPrimary()); 
        logger.log(Level.FINER, "IDSPath: " + cdis_new.properties.getProperty("IDSPathId")); 
        logger.log(Level.FINER, "PDFPath: " + cdis_new.properties.getProperty("PDFPathId")); 
      
        
        
        // Insert into Media Master
        // Insert into MediaRendition
        // Insert into MediaFiles
        // Insert into MediaXrefs
        
        CallableStatement stmt = null;
            
        try {

            // Call stored procedure to create media record in TMS
            stmt = cisConn.prepareCall("{ call CreateMediaRecords(?,?,?,?,?,?,?,?,?,?)}");
                        
            stmt.setString(1, siAsst.getUoiid());
            
            if (fileType.equalsIgnoreCase("PDF")) {
            	stmt.setString(2, siAsst.getOwningUnitUniqueName() + ".pdf");
            	stmt.setString(3, cdis_new.properties.getProperty("PDFPathId"));
            }
            else {
            	stmt.setString(2, siAsst.getOwningUnitUniqueName());
            	stmt.setString(3, cdis_new.properties.getProperty("IDSPathId"));
            }
                 
            stmt.setString(4, tmsRendition.getRenditionNumber());
            stmt.setInt(5, tmsObject.getObjectID());
            stmt.setInt(6, tmsRendition.getRank());
            stmt.setInt(7, tmsRendition.getPixelH());
            stmt.setInt(8, tmsRendition.getPixelW());
            
            if (tmsRendition.getIsPrimary() ) {
                stmt.setInt(9, 1);
            }
            else {
                stmt.setInt(9, 0);
            }
            
            stmt.setString(10, renditionDate);
            
            stmt.executeUpdate();
            
        }catch(SQLException sqlex) {
		sqlex.printStackTrace();
                return false;
	}
        finally {
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
                   
        return true;
        
    }
    
}
