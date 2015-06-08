/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.sql.CallableStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import edu.si.CDIS.CIS.Database.MediaRenditions;
import edu.si.CDIS.CIS.Database.TMSObject;
import edu.si.CDIS.CIS.Database.MediaMaster;
import edu.si.CDIS.CIS.Database.MediaFiles;
import edu.si.CDIS.CIS.Database.MediaXrefs;
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
    
    
    private boolean formatNewRenditionNumber (CDIS cdis, String damsImageFileName, MediaRenditions mediaRendition) {
        
        logger.log(Level.FINER, "Dams Image fileName before formatting: {0}", damsImageFileName);
        String tmpRenditionNumber = null;
        
        String tmsDelimiter = cdis.properties.getProperty("tmsDelimiter");
        String damsDelimiter = cdis.properties.getProperty("damsDelimiter");
        
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
            mediaRendition.setRenditionNumber(tmpRenditionNumber);
        }
        else if (! tmsDelimiter.equals (damsDelimiter) ) {
            mediaRendition.setRenditionNumber (damsImageFileName.replaceAll(damsDelimiter, tmsDelimiter));      
        }
        else if (tmsDelimiter.equals (damsDelimiter)) {
            mediaRendition.setRenditionNumber(damsImageFileName);
        }
        else {
            logger.log(Level.FINER, "unable to create Rendition number, Invalid name formatting option: {0}", cdis.properties.getProperty("newRenditionNameFormat"));
            return false;
        }
        
        logger.log(Level.FINER, "Formatted name: {0}", mediaRendition.getRenditionNumber());
        
        return true;
        
    }
    
    public boolean create (CDIS cdis, SiAssetMetaData siAsst, MediaRenditions mediaRendition, TMSObject tmsObject) {
 
        this.cisConn = cdis.cisConn;    
        this.damsConn = cdis.damsConn;
        
        boolean objectPopulated = false;
        boolean MediaCreated;
        
        MediaXrefs mediaXrefs = new MediaXrefs();
        MediaFiles mediaFiles = new MediaFiles();
       
        //Get the rendition name, and the dimensions
        // if the barcode is set, use the name to get the barcode info,
        //else use the name to get rendition name with the rendition   
        String damsImageFileName = mediaFiles.populateFromDams(siAsst.getUoiid(), damsConn);
        
        if (damsImageFileName.isEmpty()) {
            logger.log(Level.FINER, "unable to retrieve media file, returning");
            return false;
        }
        
        if (! damsImageFileName.contains(".")) {
            logger.log(Level.FINER, "unable to determine filetype from filename, returning");
            return false;
        }
        
        damsImageFileName = damsImageFileName.substring(0, damsImageFileName.lastIndexOf("."));
        String fileType = damsImageFileName.substring(damsImageFileName.lastIndexOf(".")+1, damsImageFileName.length());
        
        mediaXrefs.calculateRank(damsImageFileName);
        
        // If we are dealing with barcode logic, the name of the rendition that we are mapping to in TMS,
        // and the objectID is populated by an alternate method
        if (cdis.properties.getProperty("mapFileNameToBarcode").equals("true")) {
            objectPopulated = tmsObject.mapFileNameToBarcode(damsImageFileName, cisConn);
                
            if (objectPopulated) {
                
                // For NASM, we have to append the timestamp to the renditionName only on barcoded objects for uniqueness
                if (cdis.properties.getProperty("appendTimeToNumber").equals("true"))  {
                    DateFormat df = new SimpleDateFormat("kkmmss");
                    mediaRendition.setRenditionNumber(tmsObject.getObjectID() + "_" + String.format("%03d", mediaXrefs.getRank()) + "_" + df.format(new Date()));
                }
                else {
                    // For barcode objects, the renditionNumber is the objectID plus the rank
                    mediaRendition.setRenditionNumber(tmsObject.getObjectID() + "_" + mediaXrefs.getRank() );
                }
            }
        }
        
        if (! objectPopulated) {
            if (cdis.properties.getProperty("mapFileNameToObjectNumber").equals("true")) {
            
                objectPopulated = tmsObject.mapFileNameToObjectNumber(damsImageFileName, cdis);
                if (objectPopulated) {
                    formatNewRenditionNumber (cdis, damsImageFileName, mediaRendition);
                }
 
            }
        }
        
        if (! objectPopulated) {
            if (cdis.properties.getProperty("mapFileNameToObjectID").equals("true")) {
               
                objectPopulated = tmsObject.mapFileNameToObjectID(damsImageFileName, cisConn);
                if (objectPopulated) {
                    mediaRendition.setRenditionNumber(damsImageFileName); 
                }
                
            }
        }
        
        if (! objectPopulated) {
                // we were unable to populate the object, return with a failure indicator

                //Set the RenditionNumber as the filename for reporting purposes
                mediaRendition.setRenditionNumber(damsImageFileName);
                logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain object Data");
                return false;
        }
        
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
        String renditionDate = df1.format(cal.getTime());
        
        // Set the primaryRenditionFlag
        mediaXrefs.populateIsPrimary(tmsObject.getObjectID(), cisConn);
        
        logger.log(Level.FINER, "about to create TMS media Record:");
        logger.log(Level.FINER, "ObjectID: " + tmsObject.getObjectID());
        logger.log(Level.FINER, "RenditionNumber: {0}", mediaRendition.getRenditionNumber());
        logger.log(Level.FINER, "Rank: " + mediaXrefs.getRank());
        logger.log(Level.FINER, "PixelH: " + mediaFiles.getPixelH());
        logger.log(Level.FINER, "PixelW: " + mediaFiles.getPixelW());
        logger.log(Level.FINER, "IsPrimary: " + mediaXrefs.getIsPrimary()); 
        logger.log(Level.FINER, "IDSPath: " + cdis.properties.getProperty("IDSPathId")); 
        logger.log(Level.FINER, "PDFPath: " + cdis.properties.getProperty("PDFPathId")); 
      
        // Insert into the MediaMaster table
        MediaMaster mediaMaster = new MediaMaster();
        mediaMaster.insertNewRecord();
        
        // Insert into MediaRenditions
        mediaRendition.insertNewRecord();
        
        // Insert into MediaFiles
        mediaFiles.insertNewRecord();
        
        // Insert into MediaXrefs
        mediaXrefs.insertNewRecord();
        
        CallableStatement stmt = null;
            
        try {

            // Call stored procedure to create media record in TMS
            stmt = cisConn.prepareCall("{ call CreateMediaRecords(?,?,?,?,?,?,?,?,?,?)}");
                        
            stmt.setString(1, siAsst.getUoiid());
            
            if (fileType.equalsIgnoreCase("PDF")) {
            	stmt.setString(2, siAsst.getOwningUnitUniqueName() + ".pdf");
            	stmt.setString(3, cdis.properties.getProperty("PDFPathId"));
            }
            else {
            	stmt.setString(2, siAsst.getOwningUnitUniqueName());
            	stmt.setString(3, cdis.properties.getProperty("IDSPathId"));
            }
                 
            stmt.setString(4, mediaRendition.getRenditionNumber());
            stmt.setInt(5, tmsObject.getObjectID());
            stmt.setInt(6, mediaXrefs.getRank());
            stmt.setInt(7, mediaFiles.getPixelH());
            stmt.setInt(8, mediaFiles.getPixelW());
            
            if (mediaXrefs.getIsPrimary() ) {
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
