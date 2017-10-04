/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.CisRecordAttr;
import edu.si.damsTools.cdis.cis.CisRecordFactory;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.dams.database.Uois;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.logging.Level;
import edu.si.damsTools.cdis.dams.database.SiPreservationMetadata;

/**
 *
 * @author rfeldman
 */
public class DamsRecord {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final Uois uois;
    private final SiAssetMetadata siAsst;
    
    public Uois getUois () {
        return uois;
    }
    
    public DamsRecord() {
        uois = new Uois();
        siAsst = new SiAssetMetadata();
    }
    
    public void setUoiId(String uoiId) {
        uois.setUoiid(uoiId);
        siAsst.setUoiid(uoiId);
    }
    
    public void setBasicData() {
        uois.populateName();
        siAsst.populateSiAsstData();
    } 
    
    

}
