/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.files;

import com.twmacinta.util.MD5;
import edu.harvard.hul.ois.jhove.App;
import edu.harvard.hul.ois.jhove.JhoveBase;
import edu.harvard.hul.ois.jhove.Module;
import edu.harvard.hul.ois.jhove.RepInfo;
import edu.harvard.hul.ois.jhove.module.AiffModule;
import edu.harvard.hul.ois.jhove.module.JpegModule;
import edu.harvard.hul.ois.jhove.module.PdfModule;
import edu.harvard.hul.ois.jhove.module.TiffModule;
import edu.harvard.hul.ois.jhove.module.WaveModule;
import edu.harvard.hul.ois.jhove.module.Jpeg2000Module;
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.vfcu.utilities.ErrorLog;
 
import java.io.File;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.io.FilenameUtils;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class MediaFile {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String basePathStaging;
    private String basePathVendor;
    private String fileDate;
    private String fileName;
    private String fileSize;
    private String filePathEnding;
    private String vfcuMd5Hash;
    private Integer vfcuMediaFileId;
    private Integer vfcuMd5FileId;
    
   
    public String getBasePathStaging () {
        return this.basePathStaging;
    }
        
    public String getFileDate () {
        return this.fileDate;
    }
       
    public String getFileName () {
        return this.fileName;
    }
    
    public String getFilePathEnding () {
        return this.filePathEnding;
    }
    
    public String getFileSize () {
        return this.fileSize;
    }
    
    public String getBasePathVendor () {
        return this.basePathVendor;
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
    
    
    public void setBasePathStaging (String basePathStaging) {
        this.basePathStaging = basePathStaging;
    }
       
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    
    public void setFilePathEnding (String filePathEnding) {
        this.filePathEnding = filePathEnding;
    }
    
    public void setBasePathVendor (String vendorPath) {
        this.basePathVendor = vendorPath;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
    public void setVfcuMd5FileId (Integer vfcuMd5FileId ) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
     private boolean copyOrMoveToDamsStaging () {
        String vendorFileWithPath;
        String stagingPathDir;
        
        if (getFilePathEnding() == null) {
            vendorFileWithPath = getBasePathVendor() + "/" + getFileName ();
            stagingPathDir = getBasePathStaging() + "/"  + getFileName ();
        }
        else {
            vendorFileWithPath = getBasePathVendor() + "/" + getFilePathEnding() + "/" + getFileName ();
            stagingPathDir = getBasePathStaging() + "/" + getFilePathEnding() + "/"  + getFileName ();
        }

        logger.log(Level.FINEST, "Source: " + vendorFileWithPath);
        logger.log(Level.FINEST, "Destination " + stagingPathDir);
            
        try { 
            Path source      = Paths.get(vendorFileWithPath);
            Path destination = Paths.get(stagingPathDir);
           
            if (DamsTools.getProperty("fileXferType").equals("move") ) {
                Files.move(source, destination);
            } else {
                Files.copy(source, destination);
            }
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error in transfer of media file " + vendorFileWithPath +  " to: " + stagingPathDir, e);
            return false;
        }
        
        return true;
    }
    
    
    private boolean jhoveValidate(Module module, ArrayList paramList) {
        
        try { 
            String NAME = new String( "Jhove" );
            String RELEASE = new String( "1.16" );
            int[] DATE = new int[] { 2016, 05, 1 };
            String USAGE = new String( "no usage" );
            String RIGHTS = new String( "LGPL v2.1" );
            App app = new App( NAME, RELEASE, DATE, USAGE, RIGHTS );
            
            JhoveBase jb = new JhoveBase();
            
            String configFile = DamsTools.getDirectoryName() + "/conf/jhove_config.xml";
            jb.init( configFile, null );
             
            jb.setEncoding( "utf-8" );
            jb.setBufferSize( 131072 );
            jb.setChecksumFlag( false );
            jb.setShowRawFlag( false );
            jb.setSignatureFlag( false );
            
            module.init("");
            module.setBase(jb);
            
            module.setDefaultParams(paramList);
            
            try {
                
                String inputFileNamePath;
                
                if (getFilePathEnding() == null) {
                    inputFileNamePath = getBasePathStaging() + "/" + getFileName ();
                }
                else {
                    inputFileNamePath = getBasePathStaging() + "/" + getFilePathEnding() + "/" + getFileName () ;
                }
                
                RepInfo repInfo = new RepInfo (inputFileNamePath); 
                File inputfile = new File(inputFileNamePath);
                jb.processFile( app, module, false, inputfile, repInfo );
                
                int isValid = repInfo.getValid();
             
                if (isValid != 1) {
                    String[] pathArray;
                    pathArray = new String[] {inputFileNamePath};
                    String outputFileName = getFileName() + ".jhoveOut.txt"; 
                    
                    jb.dispatch(app, module, null, null, outputFileName, pathArray);
                    logger.log(Level.FINER, "JHOVE validation failed for " + inputFileNamePath);
                    return false;
                }
                
                logger.log(Level.FINER, "Jhove response successful");
                
            } catch ( Exception e ) {
                logger.log(Level.FINER, "Error captured in dispatch areas", e);
                return false;
            }

        } catch ( Exception e ) {
            logger.log(Level.FINER, "Error captured: ", e);
            return false;
        }
        
        return true;
    }
    
    public boolean populateMediaFileValues () {

        String sql = "SELECT  b.media_file_name, a.base_path_vendor, a.file_path_ending, b.vfcu_md5_file_id " +
                     "FROM     vfcu_md5_File a, " +
                     "         vfcu_media_file b " +
                     "WHERE    a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                     "AND      b.vfcu_media_file_id = " + getVfcuMediaFileId();
                        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try ( PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
              ResultSet rs = pStmt.executeQuery() ) {
                    
            if (rs.next()) {
                setFileName(rs.getString(1));
                setBasePathVendor(rs.getString(2));
                setFilePathEnding(rs.getString(3));
                setVfcuMd5FileId(rs.getInt(4));
            }            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain mediaFile name and path", e );
                return false;
        }
        return true;
    }
    
    public boolean populateMediaFileAttr () {

        String filePathEnding = null;
        String fileWithPath = null;
        
        if (getFilePathEnding() != null) {
            filePathEnding = "/" + getFilePathEnding();
        }
        else {
            filePathEnding = "";
        }
        
        if (DamsTools.getProperty("transferMd5File").equals("move") ) {
            fileWithPath = getBasePathStaging() + filePathEnding + "/" + getFileName ();
        }
        else {
            fileWithPath = getBasePathVendor() + filePathEnding + "/" + getFileName ();
        }
        
        Path vendorFile      = Paths.get(fileWithPath);
        
        try {
            BasicFileAttributes attr = Files.readAttributes(vendorFile, BasicFileAttributes.class);
       
            this.fileDate = attr.lastModifiedTime().toString().substring(0,10);      
            this.fileSize = Long.toString(attr.size());

        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain file attributes", e );
                return false;
        }
        
        return true;
    }
     
    public boolean generateMd5Hash () {
        
        String fileWithPath;
   
        if (getFilePathEnding() == null) {
            fileWithPath = getBasePathStaging() + "/" + getFileName ();
        }
        else {
            fileWithPath = getBasePathStaging() + "/" + getFilePathEnding() + "/" + getFileName () ;
        }
                
        try {
            //uses java fast md5
            MD5 md5 = new MD5();
            String hash = md5.asHex(md5.getHash(new File(fileWithPath)));
            
            this.vfcuMd5Hash = hash;

            logger.log(Level.FINER, "FAST Md5 hash value: " + this.vfcuMd5Hash );
               
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain faster md5 hash value", e );
                return false;
        }
        
        return true;
    }
    
    
    private boolean dupNameNotFound () {
        
        String sql = "SELECT  'X' " +
                     "FROM    vfcu_media_file a, " +
                              " vfcu_md5_file b " +
                     "WHERE   a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                     "AND     a.vfcu_md5_file_id != " + getVfcuMd5FileId() +
                     " AND    media_file_name = '" + getFileName() + "' " +
                     "AND    project_cd = '" + DamsTools.getProjectCd() + "' " +
                     "AND NOT EXISTS ( " +
                        "SELECT 'X' FROM vfcu_error_log c  " +
                        "WHERE a.vfcu_media_file_id = c.vfcu_media_file_id) ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);         
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 return (false);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for duplicate fileName", e );
                return false;
        }
        return true;
    }
    
    
    
    public void validate() {
        
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuMediaFileId(getVfcuMediaFileId() ); 
        vfcuMediaFile.populateMd5FileId();
        
        //get the media filename, we may need it for error reporting
        vfcuMediaFile.populateMediaFileName();
        
        //generateNewMd5 checksum for the file, and get the file date
        boolean hashGenerated = generateMd5Hash();
        if (!hashGenerated) {        
            ErrorLog errorLog = new ErrorLog();  
            errorLog.capture(vfcuMediaFile, "MDG", "Error: Unable to generate hash value");
            return;
        }
        
        //validate the filename (duplicate check)
        if (DamsTools.getProperty("dupFileNmCheck").equals("true")) {
            //look to see if the file already exists that is not in error state
            boolean noDupFile = dupNameNotFound();
            if (! noDupFile) {
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(vfcuMediaFile, "DUP", "Error: Duplicate File Found");
                return;
            }
        }
        
        //Get the date of the physical file
        populateMediaFileAttr();
                
        vfcuMediaFile.setVfcuChecksum(getVfcuMd5Hash());
        vfcuMediaFile.setMediaFileDate(getFileDate());
        vfcuMediaFile.setMediaFileSize(getFileSize());
        
        //record checksum and date for the file; 
        vfcuMediaFile.updateVfcuMediaAttributes();
                
        //make sure the checksum is not equivalent to a zero-byte file
        if ( getFileSize().equals("0") || getVfcuMd5Hash().equals("d41d8cd98f00b204e9800998ecf8427e") ) {
            ErrorLog errorLog = new ErrorLog();  
            errorLog.capture(vfcuMediaFile, "ZBF", "Error: Zero-Byte file calculated");
            return;
        }
 
        // Set module for jhove validation 
        Module module = null;
        
        String fileNameExtension = FilenameUtils.getExtension(this.fileName).toLowerCase();
        
        //String paramList = "byteoffset=true";
        ArrayList <String> jhoveParamList; 
        jhoveParamList  = new ArrayList <>();
            
        switch (fileNameExtension) {
            case "aif" :
            case "aiff" :
                module = new AiffModule();
                logger.log(Level.FINEST, "Module set to aiff" );
                break;
            case "tif" :
            case "tiff" :
                module = new TiffModule();
                jhoveParamList.add("byteoffset=true");
                logger.log(Level.FINEST, "Module set to tif" );
                break;
            case "jp2" : 
                module = new Jpeg2000Module();
                logger.log(Level.FINEST, "Module set to jpeg2000" );
                break;
            case "jpg" : 
            case "jpeg" :     
                module = new JpegModule();
                logger.log(Level.FINEST, "Module set to jpg" );
                break;
            case "pdf" :
                module = new PdfModule();
                logger.log(Level.FINEST, "Module set to pdf" );
                break;  
            case "wav" :
                module = new WaveModule();
                logger.log(Level.FINEST, "Module set to wav" );
                break;
        }
        
        // perform jhove validation if needed 
        if (module != null) {          
            
            boolean jhoveValidationSuccess = jhoveValidate(module, jhoveParamList);
            
            if (jhoveValidationSuccess) {
                VfcuActivityLog activityLog = new VfcuActivityLog();
                activityLog.setVfcuMediaFileId(getVfcuMediaFileId());
                activityLog.setVfcuStatusCd("JH");
                activityLog.insertRow();
            }
            else {
                ErrorLog errorLog = new ErrorLog();  
                errorLog.capture(vfcuMediaFile, "JHV", "Error - JHOVE validation failed");
                return;
            }
        }
        
        //check to see if checksum values are the same from database
        vfcuMediaFile.populateVendorChecksum();
        if (vfcuMediaFile.getVendorChecksum().equals(getVfcuMd5Hash()) ) {
            //log in the database
            VfcuActivityLog activityLog = new VfcuActivityLog();
            activityLog.setVfcuMediaFileId(getVfcuMediaFileId());
            activityLog.setVfcuStatusCd("PM");
            activityLog.insertRow();
        }
        else {
            ErrorLog errorLog = new ErrorLog();  
            errorLog.capture(vfcuMediaFile, "VMD", "MD5 checksum validation failure");
            return;
        }    
         
    }
    
    public boolean transfer() {
            
        try {
            VfcuActivityLog activityLog = new VfcuActivityLog();
            VfcuMd5File vfcuMd5File = new VfcuMd5File();
      
            //get fileName, vendor_file_path for the current id
            populateMediaFileValues();
            
            vfcuMd5File.setVfcuMd5FileId(getVfcuMd5FileId());
            boolean stagingPathPopulated = vfcuMd5File.populateStagingFilePath();
            if (! stagingPathPopulated) {
                logger.log(Level.FINEST, "Error obtaining staging path.  Cannot process file" );
                throw new Exception();
            }
            
            setFilePathEnding(vfcuMd5File.getFilePathEnding() ); 
            setBasePathStaging(vfcuMd5File.getBasePathStaging() ); 
            
            //copyOrMove to DAMS staging
            boolean fileXferred = copyOrMoveToDamsStaging();
            if (!fileXferred) {        
                throw new Exception();
            }      
               
            if (DamsTools.getProperty("fileXferType").equals("move") ) {
                activityLog.setVfcuStatusCd("MV");
            } else {
                activityLog.setVfcuStatusCd("CV");
            }    
            
            activityLog.setVfcuMediaFileId(getVfcuMediaFileId());
            activityLog.insertRow();
            
        } catch(Exception e) {
            VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
            vfcuMediaFile.setVfcuMediaFileId(getVfcuMediaFileId() );
            vfcuMediaFile.setVfcuMd5FileId(getVfcuMd5FileId());
            vfcuMediaFile.populateMediaFileName();
            
            if (DamsTools.getProperty("fileXferType").equals("move") ) {
                ErrorLog errorLog = new ErrorLog(); 
                errorLog.capture(vfcuMediaFile, "MFV", "Failure to move Vendor File");
            }
            else {
                ErrorLog errorLog = new ErrorLog(); 
                errorLog.capture(vfcuMediaFile, "CFV", "Failure to Copy Vendor File");
            }
            return false;
        }
        
        return true;
    }
}
