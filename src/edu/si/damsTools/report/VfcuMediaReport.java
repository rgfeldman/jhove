/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.report.rptFile.DataSection;
import edu.si.damsTools.report.rptFile.CompletedSection;
import edu.si.damsTools.report.rptFile.VfcuMediaFailedSection;
import edu.si.damsTools.utilities.XmlData;
import edu.si.damsTools.utilities.XmlUtils;
import edu.si.damsTools.vfcu.database.VfcuMd5FileActivityLog;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuMediaReport implements DisplayFormat {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<String> multiReportKeyValues;
     
    public VfcuMediaReport() {
       multiReportKeyValues = new ArrayList();
       logger.log(Level.FINEST,"Creating VFCUDir-based object"); 
    }
    
    public String returnDocHeader(String multiRptkeyValue) {
        return (XmlUtils.getConfigValue("projectCd").toUpperCase() + " VFCU Activity Report- " + returnRptVendorDir(Integer.valueOf(multiRptkeyValue)) );
    }
    
    public String returnEmailTitle(String multiRptkeyValue) {
        return (XmlUtils.getConfigValue("projectCd").toUpperCase() + " VFCU Activity Report- " + returnRptVendorDir(Integer.valueOf(multiRptkeyValue)) );
    }
    
    public boolean returnSuppressAttachFlag(String multiRptkeyValue) {
        return false;
    }
    
    public List<DataSection> sectionFactory() {
        List<DataSection> sections = new ArrayList<>();
        
        sections.add(new VfcuMediaFailedSection());
        sections.add(new CompletedSection());
        
        return sections;
    }
    
    public boolean updateDbComplete (String md5FileId) {
        
        VfcuMd5FileActivityLog vfcuMd5FileActivityLog = new VfcuMd5FileActivityLog();
        vfcuMd5FileActivityLog.setVfcuMd5FileId(Integer.parseInt(md5FileId));
        vfcuMd5FileActivityLog.setVfcuMd5StatusCd("RE");
        boolean rowInserted = vfcuMd5FileActivityLog.insertRecord();
        
        return true;
                
    }
    
    public String returnStatsListsHeader(String multiRptkeyValue) {
        String sql;
        String sdir = null;
        
        if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true")) {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8) || '/' || SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '/', -1, 1)-1) " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + multiRptkeyValue;
            } 
        else {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8) || '/' || file_path_ending " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + multiRptkeyValue;
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
                ResultSet rs = stmt.executeQuery() ) {

                if (rs.next()) {
                    sdir = rs.getString(1);
                }        
                else {
                    throw new Exception();
                }
              
            }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to count errors for report", e);
        }
            
        String sourceDir = "Source Directory: " + sdir;
        
        return sourceDir;
    }
    
    private String returnRptVendorDir(Integer md5FileId) {
        
        String sql = null;
        String vendorDir = null;
        
        if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true")) {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8), SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '/', -1, 1)-1) " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + md5FileId;
        } 
        else {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8), file_path_ending " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + md5FileId;
        }
                
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                if (rs.getString(2) == null) {
                    vendorDir = rs.getString(1).substring(rs.getString(1).lastIndexOf('/') + 1);
                }
                else {
                    vendorDir = rs.getString(1) + "/" + rs.getString(2);
                }
            }        
            else {
                throw new Exception();
            }
        }
        catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain vendor dir for report", e);
	}
        
        return vendorDir;
    }
    
    public boolean returnMultiReportInd() {
        return true;
    }
    
}
