/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams;

import edu.si.damsTools.DamsTools;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.dams.database.Uois;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import java.util.logging.Level;
import edu.si.damsTools.cdis.dams.database.SiPreservationMetadata;
import edu.si.damsTools.cdis.dams.database.TeamsLinks;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.MediaTypeConfigR;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class DamsRecord {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final Uois uois;
    private final SiAssetMetadata siAsst;
    
    public DamsRecord() {
        uois = new Uois();
        siAsst = new SiAssetMetadata();
    }
    
    public SiAssetMetadata getSiAssetMetadata () {
        return siAsst;
    }
    
    public Uois getUois () {
        return uois;
    }
    
    public void setUoiId(String uoiId) {
        uois.setUoiid(uoiId);
        siAsst.setUoiid(uoiId);
    }
    
    public void setBasicData() {
        uois.populateName();
        siAsst.populateSiAsstData();
    } 
    
     public boolean addPreservationData (CdisMap cdisMap) {
        
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuMediaFileId(cdisMap.getVfcuMediaFileId());
        vfcuMediaFile.populateVendorChecksum();
        
        addPreservationData(cdisMap);
        // Add the preservation information
        SiPreservationMetadata prev = new SiPreservationMetadata();
        prev.setUoiid(uois.getUoiid());
        prev.setPreservationIdNumber(vfcuMediaFile.getVendorChecksum());
        boolean preservationInserted = prev.insertRow();       
        if (!preservationInserted) {
            return false;
        }
        
        return true;
    }
     
    
    
}
