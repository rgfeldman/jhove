/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report;


import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlSqlConfig;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.database.VFCUMd5File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import edu.si.damsTools.cdis.Operation;

/**
 *
 * @author rfeldman
 */
public class Generator extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    protected DisplayFormat displayFormat;  
    Report[] reportObjectList;
    String keyValue;

    public Generator() {   
         switch (DamsTools.getOperationType()) {
            case "vfcuDirReport":
                //Get the bundle of reports
                // Get the md5 file ids for each genDisplayFormat
                displayFormat = new VfcuDirReport();
                break;
            case "timeframeReport":
                displayFormat = new TimeFrameReport();
                break;
            default:
                logger.log(Level.SEVERE, "Error: Additonal paramater required and not supplied");
        }
    }

    public void invoke () {
        
        boolean multiValueReport = displayFormat.populateMultiReportKeyValues();
        
        //If there are no key values then the report is intended on running only once per generator instance
        if (! multiValueReport) {
            Report rpt = new Report();
            rpt.generate();
        }
        else {
            //There are possibly more than one report to generate per execution, loop through the keyvalues of the report
            for (String keyValue : displayFormat.returnKeyValueList()) {
                Report rpt = new Report();
                rpt.setKeyValue(keyValue);
                logger.log(Level.FINEST, "MultRpt Key Value: " + keyValue);

                //check if all of the files have been physically moved
            
                //Generate and Send the attachment
                rpt.generate();
            
                //update the database
                boolean dbUpdated = displayFormat.updateDbComplete(keyValue);
            
                if (dbUpdated) {
                    //create emuReady.txt file
                
                }
            }
        }        
    }
    
    public List<VFCUMd5File> populateVfcuMd5List () {
        XmlSqlConfig xml = new XmlSqlConfig();
        xml.setOpQueryNodeList(DamsTools.getQueryNodeList());
        xml.setProjectCd(DamsTools.getProjectCd());
        
        xml.setQueryTag("getMasterMd5Ids"); 
        
        //VFCUMd5File vfcuMd5Files[] = new VFCUMd5File();
        List<VFCUMd5File> vfcuMd5FileList = new ArrayList<>();
        
        for (int s = 0; s < DamsTools.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            } 
        
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                while (rs.next()) { 
                    VFCUMd5File vfcuMd5File = new VFCUMd5File();
                    vfcuMd5File.setMasterMd5FileId(rs.getInt(1));
                    vfcuMd5FileList.add(vfcuMd5File);
                }
            }
            catch(Exception e) {
                logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
                return null;
            }
        
        }

        return vfcuMd5FileList;
    } 
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        reqProps.add("damsDbDriver");
        reqProps.add("damsDbPass");
        reqProps.add("damsDbUser");
        
        //add more required props here
        return reqProps;    
    }
    
}
