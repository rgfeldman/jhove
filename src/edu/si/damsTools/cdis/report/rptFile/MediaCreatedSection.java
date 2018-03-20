/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.rptFile;

import edu.si.damsTools.cdis.cis.applications.CisRecordAttr;
import edu.si.damsTools.cdis.cis.applications.CisRecordFactory;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author rfeldman
 */
public class MediaCreatedSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList<String> sectionTextData;
     
    public MediaCreatedSection () {
        sectionTextData = new ArrayList<>();
    }
    
    public String returnTitlePhrase() {
        return "Created in the CIS and Linked by CDIS";
    }
    
    public String returnXmlTag () {
        logger.log(Level.FINEST,"In mediaCreated Section"); 
        return "getCisMediaCreatedRecords";
    }
    
    public String returnEmptyListString() {
        return "There was no new media Created in the CIS";
    }
    
    public boolean generateTextForRecord(CdisMap cdisMap) {
        
        SiAssetMetadata siAsst = new SiAssetMetadata();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        CisRecordFactory cisFactory = new CisRecordFactory();
        CisRecordAttr cisAttr = cisFactory.cisChooser();
        
        if (cisAttr == null) {
            String rptInfo = getCisHierReportInfo(cdisMap);
            if (rptInfo != null ) {
                sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Created On " + rptInfo);
            }
        }
        else {
            sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Created On " + cisAttr.returnGrpInfoForReport(cdisMap));
        }
        
        return true;
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    
     // Method: populateCdisMapListToLink()
    // Purpose: Populates the list of CdisMap records that require linking using the criteria in the xml file
    private String getCisHierReportInfo(CdisMap cdisMap) {
        
        String sql = null;
        String rptInfo = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","getMediaCreatedRptInfo");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "getMediaCreatedRptInfo sql not found");
            return null;
        }

        if (sql.contains("?CISID")) {
            Pattern p = Pattern.compile("\\?CISID-([A-Z][A-Z][A-Z])\\?");
            Matcher m = p.matcher(sql);
            
            if (m.find()) {
                
                CdisCisIdentifierMap cdisCisIdentifier = new CdisCisIdentifierMap();
                cdisCisIdentifier.setCdisMapId(cdisMap.getCdisMapId());
                cdisCisIdentifier.setCisIdentifierCd(m.group(1).toLowerCase());
                cdisCisIdentifier.populateCisIdentifierValueForCdisMapIdType(); 
     
                sql = sql.replace("?CISID-" + m.group(1) + "?", cdisCisIdentifier.getCisIdentifierValue());            
            }
        }
        
        try (PreparedStatement stmt = DamsTools.getCisConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            if (rs.next()) {
                rptInfo = rs.getString(1);
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain list of Map IDs to integrate", e);
            return "";
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        return rptInfo;
    }
        
}
