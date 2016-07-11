/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.TMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.TMS.Database.MediaFiles;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.Database.CDISMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FilenameUtils;
import edu.si.CDIS.Database.CDISActivityLog;

/**
 *
 * @author rfeldman
 */
public class MediaPath {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    public boolean redirectCisMediaPath(CDISMap cdisMap) {
        
        MediaFiles mediaFiles = new MediaFiles ();
        SiAssetMetaData siAsst = new SiAssetMetaData();
        
        //Get the uoiid from the CDIS_map object and put into siAsst object
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        
        //Get the uan based on the uoiid
        siAsst.populateOwningUnitUniqueName();
        
        //pdfs need to be handled different, they have a different path in the CIS, and a different file naming convention

        String fileNameExtension = FilenameUtils.getExtension(cdisMap.getFileName()).toLowerCase();
        switch (fileNameExtension) {
            case "tif" :
            case "jpg" :
            case "jpeg" :    
                mediaFiles.setPathId (Integer.parseInt (CDIS.getProperty("IDSPathId")) );
                mediaFiles.setFileName(siAsst.getOwningUnitUniqueName());
                break;
            case "pdf" :
                mediaFiles.setPathId (Integer.parseInt (CDIS.getProperty("PDFPathId")) );
                mediaFiles.setFileName(siAsst.getOwningUnitUniqueName() + ".pdf");
                break;
            default :
                logger.log(Level.FINER, "Error: unable to determine PDF path for fileType: " + fileNameExtension);
        } 
        
        mediaFiles.setRenditionId (Integer.parseInt (cdisMap.getCisUniqueMediaId()) );
        
        boolean pathUpdated = mediaFiles.updateFileNameAndPath();
        
        if (!pathUpdated) {
            logger.log(Level.FINER, "Error, unable to update CIS media path");
            return false;
        }
                    
        return true;
    }
}
