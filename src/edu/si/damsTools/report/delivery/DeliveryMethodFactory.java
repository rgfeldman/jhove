/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.delivery;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;

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
        
        switch (XmlUtils.getConfigValue("deliveryMethod")) {
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
