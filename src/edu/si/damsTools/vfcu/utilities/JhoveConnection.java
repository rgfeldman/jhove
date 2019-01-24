/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.utilities;

import edu.harvard.hul.ois.jhove.App;
import edu.harvard.hul.ois.jhove.JhoveBase;
import edu.harvard.hul.ois.jhove.Module;
import edu.harvard.hul.ois.jhove.RepInfo;
import edu.harvard.hul.ois.jhove.module.AiffModule;
import edu.harvard.hul.ois.jhove.module.Jpeg2000Module;
import edu.harvard.hul.ois.jhove.module.JpegModule;
import edu.harvard.hul.ois.jhove.module.PdfModule;
import edu.harvard.hul.ois.jhove.module.TiffModule;
import edu.harvard.hul.ois.jhove.module.WaveModule;
import edu.si.damsTools.DamsTools;
import java.io.File;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import edu.harvard.hul.ois.jhove.Message;
import edu.si.damsTools.utilities.XmlUtils;

/**
 *
 * @author rfeldman
 */
public class JhoveConnection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Module module;
    private final ArrayList <String> paramList;
    private RepInfo repInfo;
    private String errorMessageForVfcu;
    
    public JhoveConnection() {
        paramList  = new ArrayList <>();
    }
    
    public Module getModule() {
        return this.module;
    }
    
    public ArrayList <String> getParamList() {
        return this.paramList;
    }
    
    public String getErrorMessageForVfcu () {
        if (errorMessageForVfcu == null) {
            return "Jhove execution failed";
        }
        if (errorMessageForVfcu.length() > 34 ) {
            errorMessageForVfcu = errorMessageForVfcu.substring(0,34);
        }
        return errorMessageForVfcu;
    }
    
    public boolean populateRequiredData(String mediaFileName) {
    
        String fileNameExtension = FilenameUtils.getExtension(mediaFileName.toLowerCase());
        
        switch (fileNameExtension) {
            case "aif" :
            case "aiff" :
                module = new AiffModule();
                logger.log(Level.FINEST, "Module set to aiff" );
                break;
            case "dng" :
            case "tif" :
            case "tiff" :
                module = new TiffModule();
                paramList.add("byteoffset=true");
                logger.log(Level.FINEST, "Module set to tif" );
                break;
            case "jp2" :
            case "jpf" :
            case "jpx" :
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
            default:
                return false;
        }
        
        return true;
    }
    
        
    public boolean jhoveValidate(String inputFileNamePath) {
        
        try { 
            String NAME = new String( "Jhove" );
            String RELEASE = new String( "1.20" );
            int[] DATE = new int[] { 2018, 03, 29 };
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
                
                repInfo = new RepInfo (inputFileNamePath); 
                File inputfile = new File(inputFileNamePath);
                jb.processFile( app, module, false, inputfile, repInfo );
                
                if (repInfo.getValid() == 1) {
                    logger.log(Level.FINER, "Jhove response successful");
                }
                else {
                    return isErrMessageIgnored();   
                }
                
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
    
    private boolean isErrMessageIgnored() {
        
        List<Message> messageList = repInfo.getMessage (); 
        if (messageList.isEmpty()) {
            // jhove failed if there is no repInfo message, we never skip if the message is blank, we want to report this case as eeror
            return false;
        }
        
        for (Message message : messageList) {
            
            logger.log(Level.FINER, "JHOVE messageType: " + message.toString());
            logger.log(Level.FINER, "JHOVE message: " + message.getMessage());
            
            //Make sure we are only looking at Error messages, and ignore Info Messages
            if (! message.toString().contains("ErrorMessage")) {
                logger.log(Level.FINER, "Warning message, not an error, skipping: " + message.toString());
                continue;
            }
            
            // Store the jhove message as a legitamate error that needs to be handled by VFCU (if there is no supress flag set, or if 
                //the error does not contain the supressed message, it is an error that we wish to report
            if (XmlUtils.getConfigValue("suppressJhoveErr") == null || ! message.getMessage().startsWith(XmlUtils.getConfigValue("suppressJhoveErr"))) {
                logger.log(Level.FINER, "Error message NOT suppressed: " + message.getMessage());
                errorMessageForVfcu = message.getMessage();
                return false;
            }
            
            logger.log(Level.FINER, "Error message suppressed: " + message.getMessage());
        }
        
        
        return true;
    }
}
