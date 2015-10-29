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

import edu.si.CDIS.CDIS;

/**
 *
 * @author rfeldman
 */
public class MediaFile {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String damsStagingPath;
    private String fileName;
    private String vfcuMd5Hash;
    private String vendorPath;
    private Integer vfcuMediaFileId;
    
   
    public String getDamsStagingPath () {
        return this.damsStagingPath;
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
    
    
    
    
    public void setDamsStagingPath (String damsStagingPath) {
        this.damsStagingPath = damsStagingPath;
    }
       
    public void setFileName (String FileName) {
        this.fileName = fileName;
    }
    
    public void setVendorPath (String vendorPath) {
        this.vendorPath = vendorPath;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
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
            logger.log(Level.FINEST, "Error copying media file " + fileWithPath +  " + to: " + getDamsStagingPath());
            return false;
        }
    }
    
    public boolean populateFileNameAndPath (Connection damsConn) {
        PreparedStatement pStmt = null;
        ResultSet rs = null; 
        
        try {
            String sql = "SELECT  a.media_file_name, b.vendor_file_path " +
                        "FROM     vfcu_file_batch a, " +
                        "         vfcu_md5File b " +
                        "WHERE    a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                        "AND      a.vfcu_file_batch_id = " + getVfcuMediaFileId();
                        
            
            pStmt = damsConn.prepareStatement(sql);
            pStmt.executeUpdate(sql);
            
            if (rs.next()) {
                setFileName(rs.getString(1));
                setVendorPath(rs.getString(2));
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
    
    public boolean generateMd5Hash () {
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            FileInputStream fis = new FileInputStream("c:\\loging.log");
        
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

            logger.log(Level.FINER, "Md4 hash value: ", this.vfcuMd5Hash );
             
             
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain md5 hash value", e );
                return false;
        }
        
        return true;
    }
    
    
}
