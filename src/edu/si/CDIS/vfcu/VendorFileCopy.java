/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu;

import edu.si.CDIS.CDIS;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import edu.si.CDIS.vfcu.Database.VFCUMd5File;
import edu.si.CDIS.vfcu.Database.VFCUMediaFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Iterator;
import edu.si.CDIS.vfcu.Database.VFCUActivityLog;
import edu.si.CDIS.vfcu.Database.VFCUError;
import java.util.HashMap;
import java.sql.Connection;
import edu.si.CDIS.vfcu.ErrorLog;


/**
 *
 * @author rfeldman
 */
public class VendorFileCopy {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String stagingForDAMS;
    
    
    /**
     *
     * @param cdis
     */
    public void validateAndCopy(CDIS cdis) {
    
        String vendorBatchLocation = null;
        
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        String currentDate = df.format(new Date());
        
        //Get the location from the config file.  We typically have multiple file locations, one per filetype
        String[] vendorFileDirs = cdis.properties.getProperty("vendorBatchLocation").split(",");
        
        stagingForDAMS = cdis.properties.getProperty("vfcuStagingForCDIS");
        if (stagingForDAMS.contains("?DATE?")) {
            stagingForDAMS = stagingForDAMS.replaceAll("\\?DATE\\?", currentDate);
        }
        
        //for each vendorFileDirectory in config file, process the md5 file
        for (int i = 0; i < vendorFileDirs.length; i++) {
            
            vendorBatchLocation = vendorFileDirs[i];
            
            logger.log(Level.FINEST, "Looking for md5 file in directory : " + vendorBatchLocation );
            
            if (vendorBatchLocation.contains("?DATE?")) {
                vendorBatchLocation = vendorBatchLocation.replaceAll("\\?DATE\\?", currentDate);
            }
            
            // set variables for md5file 
            VendorMd5File md5File = new VendorMd5File();
            md5File.setVendorPath(vendorBatchLocation);
           
            int numFiles = md5File.locate (cdis.damsConn);
            
            if (! (numFiles > 0)) {
                logger.log(Level.FINEST, "No .md5 File found in specified directory : " + vendorBatchLocation );
                //look in the next vendor directory listed for an .md5 file
                continue;
            }
            //see if this file is tracked in the database yet
            VFCUMd5File vfcuMd5File = new VFCUMd5File ();
            vfcuMd5File.setVendorFilePath(vendorBatchLocation);
            vfcuMd5File.setVendorMd5FileName(md5File.getFileName());
            vfcuMd5File.setSiHoldingUnit(cdis.properties.getProperty("siHoldingUnit"));
                        
            boolean md5FileInDB = vfcuMd5File.findExistingMd5File(cdis.damsConn);
            
            if (! md5FileInDB) {          
               
                //get id sequence
                boolean idSequenceObtained = vfcuMd5File.generateVfcuMd5FileId(cdis.damsConn);
                if (! idSequenceObtained) {
                    continue;
                }
                
                //append the dams staging location with the md5fileID
                md5File.setDamsStagingPath(stagingForDAMS + "\\" + vfcuMd5File.getVfcuMd5FileId());
                
                //copy md5 file from vendor area.  we should use the local copy of this file from here on 
                //because we will have control of that file
                boolean fileCopied = md5File.copyToDAMSStaging ();                            
                if (! fileCopied) {
                    //Get the next record in for loop
                    continue;
                }
                    
                //insert logging record
                boolean recordInserted = vfcuMd5File.insertRecord(cdis.damsConn);
                if (! recordInserted) {
                    continue;
                }
                    
                boolean dataExtracted = md5File.extractData (cdis.damsConn, vfcuMd5File);
                if (! dataExtracted) {
                    continue;
                }    
            }
        }
        
        //End DB setup process
        
        //Assign Files right away to a batch
        VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
        vfcuMediaFile.setMaxFiles(Integer.parseInt(cdis.properties.getProperty("vfcuMaxFilesBatch")));
        vfcuMediaFile.setVfcuBatchNumber(cdis.getBatchNumber());
        
        int rowsUpdated = vfcuMediaFile.updateVfcuBatchNumber(cdis.damsConn);
 
        if (rowsUpdated < 1 ) {
            //no files found that can be assigned to a validate and copy batch.  We have no need to go further
            logger.log(Level.FINEST, "No files found in DB that require copy and validation" );
            validateNumFiles(cdis.damsConn);
        }
        
        //Now we updated the files and assigned to current batch, commit so we lock them into current batch
        try { if ( cdis.damsConn != null)  cdis.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
                
        //create array of files just assigned to batch
        vfcuMediaFile.getFileIdsCurrentBatch(cdis.damsConn);
        
        for (Iterator<Integer> iter = vfcuMediaFile.getFilesIdsForBatch().iterator()  ; iter.hasNext();) {
            
            VFCUActivityLog activityLog = new VFCUActivityLog();
            MediaFile mediaFile = new MediaFile();
            
            mediaFile.setVfcuMediaFileId(iter.next());
                    
            vfcuMediaFile = new VFCUMediaFile();
            
            // set the ID
            vfcuMediaFile.setVfcuMediaFileId(mediaFile.getVfcuMediaFileId());
            
            //get fileName, vendor_file_path for the current id
            mediaFile.populateMediaFileValues(cdis.damsConn);
            
            mediaFile.setDamsStagingPath(stagingForDAMS + "\\" + mediaFile.getVfcuMd5FileId());
            
            boolean fileCopied = mediaFile.copyToDamsStaging();
            if (!fileCopied) {        
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(vfcuMediaFile, "VFC", "Failure to copy Vendor File", cdis.damsConn);
                continue;
            }
                
            activityLog.setVfcuStatusCd("VC");
            activityLog.setVfcuMediaFileId(mediaFile.getVfcuMediaFileId());
            activityLog.insertRow(cdis.damsConn);

            //generateNewMd5 checksum for the file, and get the file date     
            boolean hashGenerated = mediaFile.generateMd5Hash();
            if (!hashGenerated) {
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(vfcuMediaFile, "MDG", "Unable to generate hash value", cdis.damsConn);
                continue;
            }
            mediaFile.populateMediaFileDate();
            
            //record checksum for the file; 
            
            vfcuMediaFile.updateVfcuChecksumAndDate(cdis.damsConn, mediaFile);
            
            
            //check to see if checksum values are the same from database
            vfcuMediaFile.populateVendorChecksum(cdis.damsConn);
            if (vfcuMediaFile.getVendorChecksum().equals(vfcuMediaFile.getVendorChecksum()) ) {
                //log in the database
                activityLog.setVfcuStatusCd("MP");
                activityLog.insertRow(cdis.damsConn);
            }
            else {
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(vfcuMediaFile, "MVV", "MD5 checksum validation failure", cdis.damsConn);
                continue;
            }    
            
            try { if ( cdis.damsConn != null)  cdis.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            
        }

        // count the number of files in md5 table not marked complete, and the number of physical files in vendor area, and the number of files in the destination.
        //   They should all be the same.  If they are, then mark as complete.
        // 
        validateNumFiles (cdis.damsConn);
        
    }
    
    private void validateNumFiles(Connection damsConn) {
        
        // get list of md5 files not marked complete
        VFCUMd5File vfcuMd5File = new VFCUMd5File ();
        HashMap <Integer,String> idPathNotCompleted;
        idPathNotCompleted = new HashMap<> (); 
        
        idPathNotCompleted = vfcuMd5File.checkForCompleteness(damsConn);
        
        if (idPathNotCompleted.isEmpty()) {
            //done with processing
            try { if ( damsConn != null)  damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            return;
        }
        //for each md5 file that has not been processed yet, perform validations
        
        for (Integer key : idPathNotCompleted.keySet()) { 
            
            VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
            vfcuMediaFile.setVfcuMd5FileId(key);
          
            // count the number of files in DB
            int numDbFiles = vfcuMediaFile.countNumFilesForMd5ID(damsConn);
            
            MediaFile mediaFile = new MediaFile();
            
            // count the number of files in physical vendor area that are under the same id 
            //get the path of the file
            int numVendorFiles = mediaFile.countInDirectory(idPathNotCompleted.get(key));
            
            //count the number of files in staging area at the same location as the md5 path 
            int numStagingFiles = mediaFile.countInDirectory(stagingForDAMS + "\\" + key);
            
            // if all three match, then mark the batch as complete in the database, and create the ready text file
            logger.log(Level.FINEST, "number of files in DB: " + numDbFiles );
            logger.log(Level.FINEST, "number of vendor files: " + numVendorFiles);
            logger.log(Level.FINEST, "number of staged files: " +  numStagingFiles);
            
            if (numDbFiles != numVendorFiles) {
                logger.log(Level.FINEST, "number of files in DB != number of vendor files" );
            }
            else if (numDbFiles != numStagingFiles) {
                logger.log(Level.FINEST, "number of files in DB != number of staged files" );
            }
            else {
                //mark as complete
                createCdisReadyFile(stagingForDAMS + "\\" + key);
                
                //set status to completed
                vfcuMd5File.updateVfcuComplete(damsConn);
            }
        }
        
    }

    private void createCdisReadyFile (String path) {
    
        String readyFilewithPath = null;
        
        try {
                //Create the ready.txt file and put in the media location
                readyFilewithPath = path + "\\CDIS_ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);;
        }
    }
        
        
    
        
}
