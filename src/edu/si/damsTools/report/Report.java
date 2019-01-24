/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;

import edu.si.damsTools.report.rptFile.RptFile;
import edu.si.damsTools.report.delivery.DeliveryMethodFactory;
import edu.si.damsTools.report.delivery.DeliveryMethod;
import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class Report extends ReportGenerator {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    protected static String keyValue;
    
    public Report() {
         super();
    }
    
    public void setKeyValue (String keyValue) {
        this.keyValue = keyValue;
    }
    
    public boolean generate () {

        RptFile rptFile = new RptFile();
        rptFile.setKeyValue(keyValue);
        
        //Initiatlize file for attachment and create the header           
        rptFile.createFileAndHeader(displayFormat);
 
         //Obtain the data for all the sections
        boolean sectionInfo = rptFile.obtainSectionData(displayFormat);
        
        rptFile.closeDocument();
        
        if (!sectionInfo) {
            return false;
        }
        
        logger.log(Level.FINEST, "Attachment fileName: " + rptFile.getFileNameLoc());  
        
        //Set the delivery Method for the the report
        DeliveryMethodFactory deliveryMethodFact = new DeliveryMethodFactory();
        DeliveryMethod deliveryMethod = deliveryMethodFact.deliveryMethodChooser();
        
        //Craete the actual email and send the email
        boolean reportDelivered = deliveryMethod.deliver(displayFormat, rptFile);
        
        return reportDelivered;
    }
    
}
