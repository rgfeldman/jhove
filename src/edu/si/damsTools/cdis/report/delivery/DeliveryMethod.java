/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.delivery;

import edu.si.damsTools.cdis.report.rptFile.RptFile;
import edu.si.damsTools.cdis.report.DisplayFormat;

/**
 *
 * @author rfeldman
 */
public interface DeliveryMethod {
    
    public boolean deliver(DisplayFormat displayFormat, RptFile rptFile);
    
}
