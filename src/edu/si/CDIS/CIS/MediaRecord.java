/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CIS.Database.MediaRenditions;
import edu.si.CDIS.CIS.Database.TMSObject;
import edu.si.CDIS.CIS.Database.MediaMaster;
import edu.si.CDIS.CIS.Database.MediaFiles;
import edu.si.CDIS.CIS.Database.MediaXrefs;
import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.DAMS.Database.Uois;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MediaRecord {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private boolean formatNewRenditionNumber ( String damsImageFileName, MediaRenditions mediaRendition) {
        
        logger.log(Level.FINER, "Dams Image fileName before formatting: {0}", damsImageFileName);
        
        String tmsDelimiter = CDIS.getProperty("tmsDelimiter");
        String damsDelimiter = CDIS.getProperty("damsDelimiter");
        
        // If the delimeter is different from the image to the renditionNumber, we need to put the appropriate delimeter in the newly created name
        if (tmsDelimiter.equals (damsDelimiter) ) {
            mediaRendition.setRenditionNumber(damsImageFileName);
        }
        else {
            mediaRendition.setRenditionNumber (damsImageFileName.replaceAll(damsDelimiter, tmsDelimiter));
        }
        
        logger.log(Level.FINER, "Formatted name: {0}", mediaRendition.getRenditionNumber());
        
        return true;
        
    }
    
    public boolean create (Uois uois, MediaRenditions mediaRenditions, TMSObject tmsObject) {
 
        boolean objectIdPopulated = false;
        boolean returnVal;
        int updateCount = 0;
        
        MediaXrefs mediaXrefs = new MediaXrefs();
        MediaFiles mediaFiles = new MediaFiles();
       
        //Get the rendition name, and the dimensions
        // if the barcode is set, use the name to get the barcode info,
        //else use the name to get rendition name with the rendition  
        boolean uoisInfoPopulated = uois.populateUoisData();
        
        if (! uoisInfoPopulated) {
            logger.log(Level.FINER, "unable to retrieve media file, returning");
            return false;
        }
        
        if (! uois.getName().contains(".")) {
            logger.log(Level.FINER, "unable to determine filetype from filename, returning");
            return false;
        }
        
        String extensionlessFileName = uois.getName().substring(0, uois.getName().lastIndexOf("."));
        String fileType = uois.getName().substring(uois.getName().lastIndexOf(".")+1, uois.getName().length()).toLowerCase();
        
        logger.log(Level.FINER, "extensionlessFileName: " + extensionlessFileName );
        logger.log(Level.FINER, "FileType: " + fileType );
        
        mediaXrefs.calculateRank(extensionlessFileName);
        
        // If we are dealing with barcode logic, the name of the rendition that we are mapping to in TMS,
        // and the objectID is populated by an alternate method
        
        if (Integer.parseInt(CDIS.getProperty("assignToObjectID")) > 0) {
            tmsObject.setObjectID (Integer.parseInt(CDIS.getProperty("assignToObjectID")));
            objectIdPopulated = true;
            formatNewRenditionNumber (extensionlessFileName, mediaRenditions);
            logger.log(Level.FINER, "Set object to ObjectID");
        }
                
        if (! objectIdPopulated) {
            if (CDIS.getProperty("mapFileNameToBarcode").equals("true")) {
                objectIdPopulated = tmsObject.mapFileNameToBarcode(extensionlessFileName);
                
                if (objectIdPopulated) {
                
                    // For NASM, we have to append the timestamp to the renditionName only on barcoded objects for uniqueness
                    if (CDIS.getProperty("appendTimeToNumber").equals("true"))  {
                        DateFormat df = new SimpleDateFormat("kkmmss");
                        mediaRenditions.setRenditionNumber(tmsObject.getObjectID() + "_" + String.format("%03d", mediaXrefs.getRank()) + "_" + df.format(new Date()));
                    }
                    else {
                        // For barcode objects, the renditionNumber is the objectID plus the rank
                        mediaRenditions.setRenditionNumber(tmsObject.getObjectID() + "_" + mediaXrefs.getRank() );
                    }
                }
            }
        }
        if (! objectIdPopulated) {
            if (CDIS.getProperty("mapFileNameToObjectNumber").equals("true")) {
            
                objectIdPopulated = tmsObject.mapFileNameToObjectNumber(extensionlessFileName);
                if (objectIdPopulated) {
                    formatNewRenditionNumber (extensionlessFileName, mediaRenditions);
                }
 
            }
        }
        
        if (! objectIdPopulated) {
            if (CDIS.getProperty("mapFileNameToObjectID").equals("true")) {
               
                objectIdPopulated = tmsObject.mapFileNameToObjectID(uois.getName());
                if (objectIdPopulated) {
                    mediaRenditions.setRenditionNumber(extensionlessFileName); 
                }
            }
        }
        
        if (! objectIdPopulated) {
                // we were unable to populate the object, return with a failure indicator

                //Set the RenditionNumber as the filename for reporting purposes
                mediaRenditions.setRenditionNumber(extensionlessFileName);
                logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain object Data");
                return false;
        }
        
        // Set the primaryRenditionFlag
        logger.log(Level.FINER, "about to create TMS media Record:");
        logger.log(Level.FINER, "ObjectID: " + tmsObject.getObjectID());
        logger.log(Level.FINER, "RenditionNumber: {0}", mediaRenditions.getRenditionNumber());
        logger.log(Level.FINER, "Rank: " + mediaXrefs.getRank());
  
        // Insert into the MediaMaster table
        MediaMaster mediaMaster = new MediaMaster();
        returnVal = mediaMaster.insertNewRecord();
        
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaMaster table creation failed, returning");
           return false;
        }
        
        // Insert into MediaRenditions
        returnVal = mediaRenditions.insertNewRecord(mediaMaster.getMediaMasterId());
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaRenditions table creation failed, returning");
           return false;
        }
        
        // Update mediaMaster with the renditionIds
        mediaMaster.setDisplayRendId(mediaRenditions.getRenditionId());
        mediaMaster.setPrimaryRendId(mediaRenditions.getRenditionId());
        updateCount = mediaMaster.updateRenditionIds();
        if (updateCount != 1) {
            logger.log(Level.FINER, "ERROR: MediaMaster table not updated correctly, returning");
            return false;
        }
        
        // Insert into MediaFiles
        SiAssetMetaData siAsst = new SiAssetMetaData();
        siAsst.setUoiid(uois.getUoiid());
        siAsst.populateOwningUnitUniqueName();
        if (fileType.equalsIgnoreCase("PDF")) {
                mediaFiles.setPathId(Integer.parseInt (CDIS.getProperty("PDFPathId")));
                mediaFiles.setFileName (siAsst.getOwningUnitUniqueName() +  ".pdf");
        }
        else {
            mediaFiles.setPathId (Integer.parseInt (CDIS.getProperty("IDSPathId")));
            mediaFiles.setFileName(siAsst.getOwningUnitUniqueName());
        } 
        mediaFiles.setRenditionId(mediaRenditions.getRenditionId());
        returnVal = mediaFiles.insertNewRecord();
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaFiles table creation failed, returning");
           return false;
        }
        
        //Update mediaRendition with the fileId
        mediaRenditions.setFileId(mediaFiles.getFileId());
        updateCount = mediaRenditions.updateFileId();
        if (updateCount != 1) {
            logger.log(Level.FINER, "ERROR: MediaRenditions table not updated correctly, returning");
            return false;
        }
                
        // Insert into MediaXrefs
        mediaXrefs.setMediaMasterId(mediaMaster.getMediaMasterId());
        mediaXrefs.setObjectId(tmsObject.getObjectID());
        mediaXrefs.populateIsPrimary();
        logger.log(Level.FINER, "IsPrimary: " + mediaXrefs.getPrimary());
        returnVal = mediaXrefs.insertNewRecord();
        if (! returnVal) {
           logger.log(Level.FINER, "ERROR: MediaXref table creation failed, returning");
           return false;
        }
        
        logger.log(Level.FINER, "New Media Created Successfully!!");
        
                   
        return true;
        
    }
    
}
