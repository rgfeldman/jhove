/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu;

import java.io.File;
import java.io.FileInputStream;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import java.text.SimpleDateFormat;

import edu.si.CDIS.CDIS;

/**
 *
 * @author rfeldman
 */
public class MediaFile {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String damsStagingPath;
    private String fileDate;
    private String fileName;
    private String vfcuMd5Hash;
    private String vendorPath;
    private Integer vfcuMediaFileId;
    private Integer vfcuMd5FileId;
    
   
    public String getDamsStagingPath () {
        return this.damsStagingPath;
    }
        
    public String getFileDate () {
        return this.fileDate;
    }
       
    public String getFileName () {
        return this.fileName;
    }
        
    public String getVendorPath () {
        return this.vendorPath;
    }
    
    public String getVfcuMd5Hash () {
        return this.vfcuMd5Hash;
    }
     
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    public Integer getVfcuMd5FileId () {
        return this.vfcuMd5FileId;
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
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
    public void setVfcuMd5FileId (Integer vfcuMd5FileId ) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
    public boolean copyToDamsStaging () {
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
            logger.log(Level.FINEST, "Error copying media file " + fileWithPath +  " to: " + getDamsStagingPath());
            return false;
        }
    }
    
    public boolean populateMediaFileValues (Connection damsConn) {
        PreparedStatement pStmt = null;
        ResultSet rs = null; 
        
        try {
            String sql = "SELECT  b.media_file_name, a.vendor_file_path, b.vfcu_md5_file_id " +
                        "FROM     vfcu_md5_File a, " +
                        "         vfcu_media_file b " +
                        "WHERE    a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                        "AND      b.vfcu_media_file_id = " + getVfcuMediaFileId();
                        
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                setFileName(rs.getString(1));
                setVendorPath(rs.getString(2));
                setVfcuMd5FileId(rs.getInt(3));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain mediaFile name and path", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (Exception se) { se.printStackTrace(); }
        }
         
        return true;
    }
    
    public boolean populateMediaFileDate () {
        String fileWithPath = getDamsStagingPath() + "\\" + getFileName ();
        
        fileWithPath = getVendorPath() + "\\" + getFileName ();
        File vendorFile = new File(fileWithPath);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        
        this.fileDate = sdf.format(vendorFile.lastModified());
               
        logger.log(Level.FINER, "FileDate: " + this.fileDate );
        
        return true;
    }
    
    
    public boolean generateMd5Hash () {
        
        String fileWithPath = getDamsStagingPath() + "\\" + getFileName ();
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            FileInputStream fis = new FileInputStream(fileWithPath);
        
            byte[] dataBytes = new byte[1024];
             
            int nread = 0; 
            while ((nread = fis.read(dataBytes)) != -1) {
                md.update(dataBytes, 0, nread);
            };
            
            byte[] mdbytes = md.digest();
        
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < mdbytes.length; i++) {
                sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
            }

            this.vfcuMd5Hash = sb.toString();

            logger.log(Level.FINER, "Md4 hash value: " + this.vfcuMd5Hash );
             
             
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain md5 hash value", e );
                return false;
        }
        
        return true;
    }
    
    public int countInDirectory(String path) {
        int numFiles = 0;
        String pathLocation = null;
        
        File dirLocation = new File(pathLocation);
        
        if(! dirLocation.isDirectory()){ 
            //get directory listing of all files in the directory,
            logger.log(Level.FINEST, "Error, unable to locate Vendor Location Directory: " + pathLocation);
            return -1;
        }
        
        File[] listOfFiles = dirLocation.listFiles(); 
        for (File file : listOfFiles) {
            switch (FilenameUtils.getExtension(file.getName())) {
                case "tif" :
                case "iiq" :    
                    numFiles ++;
            }
        }       
           
        return numFiles;
    }
    
    
}
