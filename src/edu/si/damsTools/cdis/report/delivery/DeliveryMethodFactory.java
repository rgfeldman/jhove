/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.delivery;

import edu.si.damsTools.DamsTools;

/**
 * Class: DeliveryMethodFactory
 * Purpose: This sets the delivery method object based on the configuration file
 * 
 * @author rfeldman
 */
public class DeliveryMethodFactory {
    public DeliveryMethodFactory() {
        
    }
    
    public DeliveryMethod deliveryMethodChooser() {
        
        DeliveryMethod deliveryMethod = null;
        
        switch (DamsTools.getProperty("rptDeliveryMethod")) {
            case "email" :
                deliveryMethod = new Email();
                break;
            case "sftp" :
                deliveryMethod = new Sftp();
                break;   
        }       
        return deliveryMethod;
    }
}
