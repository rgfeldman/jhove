/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;


import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.utilities.XmlUtils;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class ReportGenerator extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    protected DisplayFormat displayFormat;  
    Report[] reportObjectList;

    public ReportGenerator() {   
       
        switch (DamsTools.getSubOperation()) {
            case "cdisVfcuDir":
                // Get the md5 file ids for each genDisplayFormat
                displayFormat = new CdisVfcuDirReport();
                break;
            case "timeframe":
                displayFormat = new TimeFrameReport();
                break;
             case "vfcuMediaFile":
                displayFormat = new VfcuMediaReport();
                break;
            default:
                logger.log(Level.SEVERE, "Critical Error: Invalid Report type");
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

                //Generate and Send the attachment
                boolean reportSuccess = rpt.generate();
            
                if (!reportSuccess) {
                    logger.log(Level.SEVERE, "Error generating report for MultRpt Key Value: " + keyValue);
                    continue;
                }
                //update the database
                boolean dbUpdated = displayFormat.updateDbComplete(keyValue);
                
            }
        }        
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        reqProps.add("sqlFile");

        switch (DamsTools.getSubOperation()) {
             case "cdisVfcuDir":
               reqProps.add("deliveryMethod");
               if (XmlUtils.getConfigValue("deliveryMethod").equals("email") ) {
                   reqProps.add("emailToList");
               }
               reqProps.add("vfcuDirRptSupressAttch");
               reqProps.add("useMasterSubPairs");
               break;
            case "timeframe":
                reqProps.add("emailToList");
                reqProps.add("rptHours"); 
                reqProps.add("timeFrameEmailList");
                reqProps.add("tfRptSupressAttch");
                break;
            case "vfcuMediaFile":
                reqProps.add("emailToList");
                reqProps.add("useMasterSubPairs");
                break;             
               
            default:
                logger.log(Level.SEVERE, "Fatal Error, Invalid report type: " + DamsTools.getSubOperation());
        }
            
        //add more required props here
        return reqProps;    
    }
    
    public boolean requireSqlCriteria () {
        return true;
    }
    
}
