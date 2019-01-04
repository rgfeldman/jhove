/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.files.xferType;

import java.nio.file.Path;
/**
 *
 * @author rfeldman
 */
public interface XferType {
    
    public String returnCompleteXferCode();
    
    public String returnFailureMessage();
    
    public String returnXferErrorCode();
    
    public boolean xferFile(Path source, Path destination);
    
}
