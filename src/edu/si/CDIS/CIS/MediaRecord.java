/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.Database.MediaRenditions;
import edu.si.CDIS.CIS.Database.Objects;
import edu.si.CDIS.CIS.Database.MediaMaster;
import edu.si.CDIS.CIS.Database.MediaFiles;
import edu.si.CDIS.CIS.Database.MediaXrefs;

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
    
    private String formatNewRenditionNumber ( String damsImageFileName) {
        
        logger.log(Level.FINER, "Dams Image fileName before formatting: {0}", damsImageFileName);
        
        String newRenditionNumber;
        
        String tmsDelimiter = CDIS.getProperty("tmsDelimiter");
        String damsDelimiter = CDIS.getProperty("damsDelimiter");
        
        // If the delimeter is different from the image to the renditionNumber, we need to put the appropriate delimeter in the newly created name
        if (tmsDelimiter.equals (damsDelimiter) ) {
            newRenditionNumber = damsImageFileName;
        }
        else {
            newRenditionNumber = damsImageFileName.replaceAll(damsDelimiter, tmsDelimiter);
        }
        logger.log(Level.FINER, "Formatted name: {0}", newRenditionNumber);
        
        return newRenditionNumber;  
    }
    
    public boolean create (Uois uois, MediaRenditions mediaRenditions, Objects tmsObject) {
 
        boolean objectIdPopulated = false;
        boolean returnSuccess;
        int updateCount = 0;
        
        MediaXrefs mediaXrefs = new MediaXrefs();
        MediaFiles mediaFiles = new MediaFiles();
       
        //Get some info from DAMS
        returnSuccess = uois.populateUoisData();
        
        if (! returnSuccess) {
            logger.log(Level.FINER, "unable to retrieve info from DAMS, returning");
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
        
        if (Integer.parseInt(CDIS.getProperty("assignToObjectID")) > 0) {
            tmsObject.setObjectID (Integer.parseInt(CDIS.getProperty("assignToObjectID")));
            objectIdPopulated = true;
            
            String newRenditionNumber = formatNewRenditionNumber (extensionlessFileName);
            mediaRenditions.setRenditionNumber(newRenditionNumber);
            
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
                    
                    String newRenditionNumber = formatNewRenditionNumber (extensionlessFileName);
                    mediaRenditions.setRenditionNumber(newRenditionNumber);
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
        returnSuccess = mediaMaster.insertNewRecord();
        
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaMaster table creation failed, returning");
           return false;
        }
        
        // Insert into MediaRenditions
        returnSuccess = mediaRenditions.insertNewRecord(mediaMaster.getMediaMasterId());
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaRenditions table creation failed, returning");
           return false;
        }
        
        // Update mediaMaster with the renditionIds
        mediaMaster.setDisplayRendId(mediaRenditions.getRenditionId());
        mediaMaster.setPrimaryRendId(mediaRenditions.getRenditionId());
        returnSuccess = mediaMaster.updateRenditionIds();
        if (! returnSuccess) {
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
        returnSuccess = mediaFiles.insertNewRecord();
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaFiles table creation failed, returning");
           return false;
        }
        
        //Update mediaRendition with the fileId
        mediaRenditions.setFileId(mediaFiles.getFileId());
        returnSuccess = mediaRenditions.updateFileId();
        if (! returnSuccess) {
            logger.log(Level.FINER, "ERROR: MediaRenditions table not updated correctly, returning");
            return false;
        }
                
        // Insert into MediaXrefs
        mediaXrefs.setMediaMasterId(mediaMaster.getMediaMasterId());
        mediaXrefs.setObjectId(tmsObject.getObjectID());
        mediaXrefs.populateIsPrimary();
        logger.log(Level.FINER, "IsPrimary: " + mediaXrefs.getPrimary());
        returnSuccess = mediaXrefs.insertNewRecord();
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaXref table creation failed, returning");
           return false;
        }
        
        logger.log(Level.FINER, "New Media Created Successfully!!");
        
        return true;
        
    }
    
}
