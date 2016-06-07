/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.TMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.TMS.Database.MediaRenditions;
import edu.si.CDIS.CIS.TMS.Database.Objects;
import edu.si.CDIS.CIS.TMS.Database.MediaMaster;
import edu.si.CDIS.CIS.TMS.Database.MediaFiles;
import edu.si.CDIS.CIS.TMS.Database.MediaXrefs;
import edu.si.CDIS.CIS.TMS.Database.MediaFormats;

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
    
    public Integer create (Uois uois, MediaRenditions mediaRenditions) {
 
        Objects tmsObject = new Objects();
        
        boolean objectIdPopulated = false;
        boolean returnSuccess;
        
        MediaXrefs mediaXrefs = new MediaXrefs();
        MediaFiles mediaFiles = new MediaFiles();
        SiAssetMetaData siAsst = new SiAssetMetaData();
        
        siAsst.setUoiid(uois.getUoiid());
        
        
        //Get some info from DAMS
        returnSuccess = uois.populateUoisData();
        
        if (! returnSuccess) {
            logger.log(Level.FINER, "unable to retrieve info from DAMS, returning");
            return 0;
        }
        
        if (! uois.getName().contains(".")) {
            logger.log(Level.FINER, "unable to determine filetype from filename, returning");
            return 0;
        }
        
        mediaFiles.setPixelH(uois.getBitmapHeight());
        mediaFiles.setPixelW(uois.getBitmapWidth());
        
        switch  (uois.getMasterObjMimeType()) {
            case "image/jpeg" :
                mediaFiles.setMediaFormatId(Integer.parseInt(CDIS.getProperty("jpgFormatId")));
                break;
            case "image/tiff" :
                mediaFiles.setMediaFormatId(Integer.parseInt(CDIS.getProperty("tifFormatId")));
                break;
            case "application/pdf":
                mediaFiles.setMediaFormatId(Integer.parseInt(CDIS.getProperty("pdfFormatId")));
                break;
            default :
                logger.log(Level.FINER, "unable to get valid mimeType from DAMS: " + uois.getMasterObjMimeType() );
                return 0;
        }
        
        //now that we have the mediaFormatId, get the mediaTypeId
        MediaFormats mediaFormats = new MediaFormats();
        mediaFormats.setMediaFormatId(mediaFiles.getMediaFormatId());
        boolean mediaTypeObtained = mediaFormats.populateMediaType();
        
        if (!mediaTypeObtained) {
            logger.log(Level.FINER, "unable to get mediaTypeId from mediaFormatId");
            return 0;
        }
        //put the mediaType we have just received into the mediaRenditions record
        mediaRenditions.setMediaTypeId(mediaFormats.getMediaTypeId());
        
        String extensionlessFileName = uois.getName().substring(0, uois.getName().lastIndexOf("."));
        logger.log(Level.FINER, "extensionlessFileName: " + extensionlessFileName );
        
        mediaXrefs.calculateRank(extensionlessFileName);
        
        if (Integer.parseInt(CDIS.getProperty("assignToObjectID")) > 0) {
            tmsObject.setObjectId (Integer.parseInt(CDIS.getProperty("assignToObjectID")));
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
                        mediaRenditions.setRenditionNumber(tmsObject.getObjectId() + "_" + String.format("%03d", mediaXrefs.getRank()) + "_" + df.format(new Date()));
                    }
                    else {
                        // For barcode objects, the renditionNumber is the objectID plus the rank
                        mediaRenditions.setRenditionNumber(tmsObject.getObjectId() + "_" + mediaXrefs.getRank() );
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
            if (CDIS.getProperty("mapAltColumnToObject").equals("true")) {
                objectIdPopulated = tmsObject.mapAltColumnToObject(uois.getUoiid());
                
                if (objectIdPopulated) {
                    String newRenditionNumber = formatNewRenditionNumber (extensionlessFileName);
                    mediaRenditions.setRenditionNumber(newRenditionNumber);  
                }

            }
        }
        
        if (! objectIdPopulated) {
                // we were unable to populate the object, return with a failure indicator

                //Set the RenditionNumber as the filename for reporting purposes
                mediaRenditions.setRenditionNumber(extensionlessFileName);
                logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain object Data");
                return -1;
        }
        
        // Set the primaryRenditionFlag
        logger.log(Level.FINER, "about to create TMS media Record:");
        logger.log(Level.FINER, "ObjectId: " + tmsObject.getObjectId());
        logger.log(Level.FINER, "RenditionNumber: {0}", mediaRenditions.getRenditionNumber());
        logger.log(Level.FINER, "Rank: " + mediaXrefs.getRank());
  
        //get the uan and the filename, we will want to check that before we add the new media
        siAsst.populateOwningUnitUniqueName();
        if (uois.getMasterObjMimeType().equals("application/pdf")) {
                mediaFiles.setPathId(Integer.parseInt (CDIS.getProperty("PDFPathId")));
                mediaFiles.setFileName (siAsst.getOwningUnitUniqueName() +  ".pdf");
        }
        else {
            mediaFiles.setPathId (Integer.parseInt (CDIS.getProperty("IDSPathId")));
            mediaFiles.setFileName(siAsst.getOwningUnitUniqueName());
        } 
        
        //check if a record with the filename as the UAN exists before we create new media
        int existingFileId = mediaFiles.returnIDForFileName();
        if (existingFileId > 0) {
            //mark as error
            return 0;
        }
        
        //check if a record with the renditionNumber to create already exists before we create the new media
        int existingRenditionId = mediaRenditions.returnIDForRenditionNumber();
        if (existingRenditionId > 0) {
            //mark as error
            return 0;
        }
        
        // Insert into the MediaMaster table
        MediaMaster mediaMaster = new MediaMaster();

        //get the correct publicAccess value based on the is_restricted value in DAMS
        // The only time the public access should not be set in TMS, is when the restricted flag in DAMS is set to "YES"
        // The default behavior in DAMS IS PUBLIC
        siAsst.populateIsRestricted();
        if (siAsst.getIsRestricted() == null ) {
            mediaMaster.setPublicAccess(1);
        }
        else if (siAsst.getIsRestricted() == "Yes" ) {
             mediaMaster.setPublicAccess(0);
        }
        else  {
           mediaMaster.setPublicAccess(1);    
        }
        
        returnSuccess = mediaMaster.insertNewRecord();
        
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaMaster table creation failed, returning");
           return 0;
        }
        
        // Insert into MediaRenditions
        returnSuccess = mediaRenditions.insertNewRecord(mediaMaster.getMediaMasterId());
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaRenditions table creation failed, returning");
           return 0;
        }
        
        // Update mediaMaster with the renditionIds
        mediaMaster.setDisplayRendId(mediaRenditions.getRenditionId());
        mediaMaster.setPrimaryRendId(mediaRenditions.getRenditionId());
        returnSuccess = mediaMaster.updateRenditionIds();
        if (! returnSuccess) {
            logger.log(Level.FINER, "ERROR: MediaMaster table not updated correctly, returning");
            return 0;
        }
        
        // Insert into MediaFiles     
        mediaFiles.setRenditionId(mediaRenditions.getRenditionId());
        returnSuccess = mediaFiles.insertNewRecord();
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaFiles table creation failed, returning");
           return 0;
        }
        
        //Update mediaRendition with the fileId
        mediaRenditions.setFileId(mediaFiles.getFileId());
        returnSuccess = mediaRenditions.updateFileId();
        if (! returnSuccess) {
            logger.log(Level.FINER, "ERROR: MediaRenditions table not updated correctly, returning");
            return 0;
        }
                
        // Insert into MediaXrefs
        mediaXrefs.setMediaMasterId(mediaMaster.getMediaMasterId());
        mediaXrefs.setObjectId(tmsObject.getObjectId());
        mediaXrefs.populateIsPrimary();
        logger.log(Level.FINER, "IsPrimary: " + mediaXrefs.getPrimary());
        returnSuccess = mediaXrefs.insertNewRecord();
        if (! returnSuccess) {
           logger.log(Level.FINER, "ERROR: MediaXref table creation failed, returning");
           return 0;
        }
        
        logger.log(Level.FINER, "New Media Created Successfully!!");
        
        return tmsObject.getObjectId();
        
    }
    
}
