/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.tms.database.MediaRenditions;
import edu.si.damsTools.cdis.cis.tms.database.MediaMaster;
import edu.si.damsTools.cdis.cis.tms.database.MediaFiles;
import edu.si.damsTools.cdis.cis.tms.database.MediaXrefs;
import edu.si.damsTools.cdis.cis.tms.database.MediaFormats;
import edu.si.damsTools.cdis.cis.tms.modules.ModuleType;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.utilities.StringUtils;
import edu.si.damsTools.utilities.XmlUtils;

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
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String errorCode;
    private final MediaFiles mediaFiles;
    private final MediaFormats mediaFormats;
    private final MediaMaster mediaMaster;
    private final MediaRenditions mediaRenditions;
    private final MediaXrefs mediaXrefs;

    
    public MediaRecord () {
         mediaFiles = new MediaFiles();
         mediaMaster = new MediaMaster();
         mediaXrefs = new MediaXrefs();
         mediaRenditions = new MediaRenditions();
         mediaFormats = new MediaFormats();
    }
    
    public String getErrorCode (){
        return this.errorCode;
    }
    
    public MediaRenditions getMediaRenditions() {
        return this.mediaRenditions;
    }
    
   

    
    /*  Method :        create    
        Description:    This method will do all it takes in TMS to create the new media, and attach it to the module passed in
    */
    public boolean create (DamsRecord damsRecord, ModuleType module) {

        // First populate mediaFiles info from DAMS (basic data)
        boolean selectedFromDams = mediaFiles.setValuesFromDams(damsRecord);
        if (! selectedFromDams) {
            logger.log(Level.FINER, "ERROR: unable to set FormatID values based on DAMS");
            errorCode = "SELDAM";
            return false;
        }
        
        //Now check if a record with the filename as the UAN exists before we create new media
        int existingFileId = mediaFiles.returnIDForFileName();
        if (existingFileId > 0) {
            errorCode = "AFRIDS";
            return false;
        }
        
        //Calculate the rank, we will need it for Rendition naming
        mediaXrefs.calculateRankFromDams(damsRecord);
        
        //Calculate the RenditionNumber
        mediaRenditions.calculateRenditionNumber(damsRecord, module);
        if (mediaRenditions.getRenditionNumber() == null ) {
            logger.log(Level.FINER, "ERROR: unable to set Rendtion number");
            errorCode = "INSCISM";
            return false;
        }
        
        //now that we know what the renditionNumber is, 
        // check if a record with the renditionNumber to create already exists before we create the new media
        if (XmlUtils.getConfigValue("dupRenditionCheck").equals("true") ) {
            int existingRenditionId = mediaRenditions.returnIDForRenditionNumber();
            if (existingRenditionId > 0) {
                errorCode = "AFRCIS";
                return false;
            }
        }

        mediaMaster.setPublicAccessFromDams(damsRecord);
        boolean rowInserted = mediaMaster.insertNewRecord();
        if (! rowInserted) {
           logger.log(Level.FINER, "ERROR: MediaMaster table creation failed, returning");
           errorCode = "INSCISM";
           return false;
        }
        
        // Now we have the format ID calculated from DAMS, get the mediaType
        mediaFormats.setMediaFormatId(mediaFiles.getMediaFormatId());
        mediaFormats.populateMediaType();
        if (mediaFormats.getMediaTypeId() == null) {
            logger.log(Level.FINER, "ERROR: unable to set MediaTypeID values based on DAMS");
            errorCode = "SELDAM";
            return false;
        }
        mediaRenditions.setMediaTypeId(mediaFormats.getMediaTypeId());
        mediaRenditions.populateRemarksFromDams(damsRecord);
       
        // Insert into MediaRenditions
        rowInserted = mediaRenditions.insertNewRecord(mediaMaster.getMediaMasterId());
        if (! rowInserted) {
           logger.log(Level.FINER, "ERROR: MediaRenditions table creation failed, returning");
           errorCode = "INSCISM";
           return false;
        }
        
        // Update mediaMaster with the renditionIds
        mediaMaster.setDisplayRendId(mediaRenditions.getRenditionId());
        mediaMaster.setPrimaryRendId(mediaRenditions.getRenditionId());
        boolean rowUpdated = mediaMaster.updateRenditionIds();
        if (! rowInserted) {
            logger.log(Level.FINER, "ERROR: MediaMaster table not updated correctly, returning");
            errorCode = "INSCISM";
            return false;
        }
        
        // Insert into MediaFiles  
        mediaFiles.setRenditionId(mediaRenditions.getRenditionId());
        rowInserted = mediaFiles.insertNewRecord();
        if (! rowInserted) {
           logger.log(Level.FINER, "ERROR: MediaFiles table creation failed, returning");
           //INSCISM
           return false;
        }
        
        //Update mediaRendition with the fileId
        mediaRenditions.setFileId(mediaFiles.getFileId());
        rowUpdated = mediaRenditions.updateFileId();
        if (! rowInserted) {
            logger.log(Level.FINER, "ERROR: MediaRenditions table not updated correctly, returning");
            errorCode = "INSCISM";
            return false;
        }
                
        // Insert into MediaXrefs
        mediaXrefs.setMediaMasterId(mediaMaster.getMediaMasterId());
        mediaXrefs.setObjectId(module.returnRecordId());
        mediaXrefs.setTableId(module.returnTableId());
        mediaXrefs.populateIsPrimary();
        logger.log(Level.FINER, "IsPrimary: " + mediaXrefs.getPrimary());
        rowInserted = mediaXrefs.insertNewRecord();
        if (! rowInserted) {
           logger.log(Level.FINER, "ERROR: MediaXref table creation failed, returning");
           errorCode = "INSCISM";
           return false;
        }
        
        logger.log(Level.FINER, "New Media Created Successfully!!");
        
        return true;    
    }
    
}
