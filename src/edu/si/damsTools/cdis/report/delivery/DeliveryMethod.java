/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.delivery;

import edu.si.damsTools.cdis.report.rptFile.RptFile;
import edu.si.damsTools.cdis.report.DisplayFormat;

/**
 * Interface: DeliveryMethod
 * Purpose: This is the interface that is used to invoke the different deliveryMethod types that are implemented, as there are multiple ways to 
 * deliver a report file
 *          
 * @author rfeldman
 */
public interface DeliveryMethod {
    
    public boolean deliver(DisplayFormat displayFormat, RptFile rptFile);
    
}
