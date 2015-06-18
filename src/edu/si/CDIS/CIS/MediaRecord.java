/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.text.SimpleDateFormat;
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
        boolean returnVal;
        int updateCount = 0;
        
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
        
        String extensionlessFileName = damsImageFileName.substring(0, damsImageFileName.lastIndexOf("."));
        String fileType = damsImageFileName.substring(damsImageFileName.lastIndexOf(".")+1, damsImageFileName.length()).toLowerCase();
        
        logger.log(Level.FINER, "extensionlessFileName: " + extensionlessFileName );
        logger.log(Level.FINER, "FileType: " + fileType );
        
        mediaXrefs.calculateRank(extensionlessFileName);
        
        // If we are dealing with barcode logic, the name of the rendition that we are mapping to in TMS,
        // and the objectID is populated by an alternate method
        if (cdis.properties.getProperty("mapFileNameToBarcode").equals("true")) {
            objectPopulated = tmsObject.mapFileNameToBarcode(extensionlessFileName, cisConn);
                
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
            
                objectPopulated = tmsObject.mapFileNameToObjectNumber(extensionlessFileName, cdis);
                if (objectPopulated) {
                    formatNewRenditionNumber (cdis, extensionlessFileName, mediaRendition);
                }
 
            }
        }
        
        if (! objectPopulated) {
            if (cdis.properties.getProperty("mapFileNameToObjectID").equals("true")) {
               
                objectPopulated = tmsObject.mapFileNameToObjectID(damsImageFileName, cisConn);
                if (objectPopulated) {
                    mediaRendition.setRenditionNumber(extensionlessFileName); 
                }
                
            }
        }
        
        if (! objectPopulated) {
                // we were unable to populate the object, return with a failure indicator

                //Set the RenditionNumber as the filename for reporting purposes
                mediaRendition.setRenditionNumber(extensionlessFileName);
                logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain object Data");
                return false;
        }
        
        // Set the primaryRenditionFlag
        mediaXrefs.populateIsPrimary(tmsObject.getObjectID(), cisConn);
        
        logger.log(Level.FINER, "about to create TMS media Record:");
        logger.log(Level.FINER, "ObjectID: " + tmsObject.getObjectID());
        logger.log(Level.FINER, "RenditionNumber: {0}", mediaRendition.getRenditionNumber());
        logger.log(Level.FINER, "Rank: " + mediaXrefs.getRank());
        //logger.log(Level.FINER, "PixelH: " + mediaFiles.getPixelH());
        //logger.log(Level.FINER, "PixelW: " + mediaFiles.getPixelW());
        logger.log(Level.FINER, "IsPrimary: " + mediaXrefs.getIsPrimary()); 
        //logger.log(Level.FINER, "IDSPath: " + cdis.properties.getProperty("IDSPathId")); 
        //logger.log(Level.FINER, "PDFPath: " + cdis.properties.getProperty("PDFPathId")); 
      
        // Insert into the MediaMaster table
        MediaMaster mediaMaster = new MediaMaster();
        returnVal = mediaMaster.insertNewRecord(cisConn);
        
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaMaster table creation failed, returning");
           return false;
        }
        
        // Insert into MediaRenditions
        returnVal = mediaRendition.insertNewRecord(cdis, mediaMaster.getMediaMasterId(),  mediaRendition.getRenditionNumber());
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaRendition table creation failed, returning");
           return false;
        }
        
        // Update mediaMaster with the renditionIds
        updateCount = mediaMaster.setRenditionIds(cisConn, mediaRendition.getRenditionId(), mediaMaster.getMediaMasterId() );
        if (updateCount != 1) {
            logger.log(Level.FINER, "ERROR: MediaMaster table not updated correctly, returning");
            return false;
        }
        
        // Insert into MediaFiles
        returnVal = mediaFiles.insertNewRecord(cdis, siAsst.getOwningUnitUniqueName(), mediaRendition.getRenditionId(), fileType,  mediaFiles.getPixelH(),  mediaFiles.getPixelW() );
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaFiles table creation failed, returning");
           return false;
        }
        
        //Update mediaRendition with the fileId
        updateCount = mediaRendition.setFileId(cisConn, mediaRendition.getRenditionId(), mediaFiles.getFileId());
        if (updateCount != 1) {
            logger.log(Level.FINER, "ERROR: MediaRendition table not updated correctly, returning");
            return false;
        }
                
        // Insert into MediaXrefs
        returnVal = mediaXrefs.insertNewRecord(cisConn, mediaMaster.getMediaMasterId(), tmsObject.getObjectID() );
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaXref table creation failed, returning");
           return false;
        }
        
        logger.log(Level.FINER, "New Media Created Successfully!!");
        
                   
        return true;
        
    }
    
}
