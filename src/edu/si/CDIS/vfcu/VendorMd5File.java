/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.vfcu.Database.VFCUMd5File;
import java.io.File;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import java.io.FileInputStream;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.IOUtils;
import edu.si.CDIS.vfcu.Database.VFCUMediaFile;
import edu.si.CDIS.vfcu.Database.VFCUActivityLog;

/**
 *
 * @author rfeldman
 */
public class VendorMd5File {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String damsStagingPath;
    private String fileName;
    private String vendorPath;
    
    public String getFileName () {
        return this.fileName;
    }
   
    public String getDamsStagingPath () {
        return this.damsStagingPath;
    }
    
    public String getVendorPath () {
        return this.vendorPath;
    }
    
    public void setDamsStagingPath (String damsStagingPath) {
        this.damsStagingPath = damsStagingPath;
    }
        
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
        
    public void setVendorPath (String vendorPath) {
        this.vendorPath = vendorPath;
    }
    
    
    public boolean copyToDAMSStaging () {
        
        String fileWithPath = null;
                
        try { 
        
            fileWithPath = getVendorPath() + "\\" + getFileName ();
            File sourceFile = new File(fileWithPath);
        
            File destPath = new File (getDamsStagingPath() );
        
            logger.log(Level.FINEST, "Copy From: " + fileWithPath);
            logger.log(Level.FINEST, "Copy To Directory: " + getDamsStagingPath());
            
            FileUtils.copyFileToDirectory(sourceFile, destPath, true);
        
            return true;
       
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error copying md5 file " + fileWithPath +  " + to: " + getDamsStagingPath());
            return false;
        }
    }
    
    
    public boolean extractData (Connection damsConn, VFCUMd5File vfcuMd5File) {
        
        String fileWithPath = null;
        LineIterator lt = null;
        
        
        try {
            fileWithPath = getDamsStagingPath() + "\\" + getFileName();
            
            FileInputStream fin= new FileInputStream(fileWithPath);
            lt = IOUtils.lineIterator(fin, "utf-8");

            while(lt.hasNext()) {
                String line=lt.nextLine();
                String[] md5HashWithFileName = line.split("  ");
                
                VFCUMediaFile vfcuMediaFile = new VFCUMediaFile();
                vfcuMediaFile.setVendorCheckSum(md5HashWithFileName[0]);
                vfcuMediaFile.setMediaFileName(md5HashWithFileName[1]);
                vfcuMediaFile.setVfcuMd5FileId(vfcuMd5File.getVfcuMd5FileId());
                
                vfcuMediaFile.generateMediaFileId(damsConn);
                vfcuMediaFile.insertRow(damsConn);
                
                VFCUActivityLog vfcuActivityLog = new VFCUActivityLog();
                vfcuActivityLog.setVfcuStatusCd("VI");
                vfcuActivityLog.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
                
                vfcuActivityLog.insertRow(damsConn);
                
            }
        
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error reading md5 file " + fileWithPath);
            return false;
        } finally {
            lt.close();
        }
        
        return true;   
                  
    }
    
    public int locate (Connection damsConn) {
        
        File dirLocation = new File(getVendorPath());
        
        if(! dirLocation.isDirectory()){ 
            //get directory listing of all files in the directory,
            logger.log(Level.FINEST, "Error, unable to locate Vendor Location Directory: " + getVendorPath());
            return -1;
        }
        
        int numFiles = 0;
        File[] listOfFiles = dirLocation.listFiles(); 
        for (File file : listOfFiles) {
            if (file.toString().endsWith(".md5")) {
                                
                // if we have an md5 file at the location, save the info 
                setFileName (file.getName()); 
                
                logger.log(Level.FINEST, "Found md5 file: " + getFileName());
                
                numFiles ++;
            }    
        }       
           
        return numFiles;
    }
    
}
