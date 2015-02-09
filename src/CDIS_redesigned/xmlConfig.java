/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned;

import java.io.File;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import java.util.HashMap;

/**
 *
 * @author rfeldman
 */
public class xmlConfig {
    
    Document doc;
    HashMap <String,String[]> SelectStmtHash ;
    
    public HashMap <String,String[]> getSelectStmtHash() {
        return this.SelectStmtHash;
    }
       
    private void addSelectStmtHash (String SelectStmt, String[] selectType) {
        this.SelectStmtHash.put(SelectStmt, selectType);
    }
        
    public void read(String metaDataXmlFile) {
        //need to use reflection invoke

        System.out.println("In MetaData sync Redesigned code");
        
        //initialize the hashmap
        this.SelectStmtHash = new HashMap <String, String[]>();
    
        try {
            //Class cls = Class.forName("CDIS_redesigned.MetaDataTarget");
            //Object obj = cls.newInstance();
            
            //Locate the metaData xml file
            File file = new File(metaDataXmlFile);
        
            //Set up the doc which will hold the xml file
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            this.doc = db.parse(file);
            doc.getDocumentElement().normalize();

            System.out.println("Root element " + doc.getDocumentElement().getNodeName());
            
            getElementByTag ("query");
       
        } catch (Exception e) {
            e.printStackTrace();
        }   

    }
    
    private void getElementByTag (String tagName) {
        
            // Move this part to a new method, and call it twice, once for each query tag
            // Look for the query tags
            NodeList nodeLst = this.doc.getElementsByTagName(tagName);
            
            String selectSql = null;
            String selectType[] = new String[2];
            
            // For each query tag found....
            for (int s = 0; s < nodeLst.getLength(); s++) {

                Node fstNode = nodeLst.item(s);
        
                if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
                    selectSql = fstNode.getTextContent();
                    
                    Element e = (Element)fstNode;
                    String queryType = e.getAttribute("type");
                    String delimiter = e.getAttribute("delimiter");
                    
                    System.out.println("SQL: " + selectSql);
                   
                    selectType[0] = tagName;
                    selectType[1] = delimiter;
                    
                    System.out.println("SQL Query Type: " + selectType[0]);
                    System.out.println("SQL Query delimiter: " + selectType[1]);
                    
                    if (selectSql != null ) { 
                        // populate objects attribute with the SQL that was obtained
                        //addSelectStmt(selectSql);
                        addSelectStmtHash(selectSql, selectType);
                    }
                    
                }
            }
    }
    
}
