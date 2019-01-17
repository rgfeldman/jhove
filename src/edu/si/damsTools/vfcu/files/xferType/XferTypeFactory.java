/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.files.xferType;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;

/**
 *
 * @author rfeldman
 */
public class XferTypeFactory {
    
    public XferType XferTypeChooser() {
        XferType xferType = null;
        
        switch (XmlUtils.getConfigValue("fileXferType")) {
            case "move" :
                xferType = new XferMove();  
                break;
            case "copy" :
                xferType = new XferCopy();  
                break;
        }

        return xferType;
    }
}
