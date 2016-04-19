# cdis
Collections - DAMS Integration System

CDIS Overview:
CDIS is a suite of batch process that integrates the Digital Asset Management System (DAMS) with the SI unit's Collection Information System (CIS).
The batch processes are run on a nightly basis, but can also be manually executed.
  
  
The batch process operation types are as follows:


* SendToHotFolder  (Copies media to the appropriate DAMS hotfolder so it can be ingested into DAMS).

* LinkToDAMS	  (Establishes the link from media already in DAMS to a CDIS_MAP record and records the DAMS unique ID).

* LinkToCISMdpp   (Establishes a link from DAMS/CDIS media back to the CIS system by recording the CIS unique ID).

* IngestToCIS   (Adds the CIS media record of a DAMS image to an existing object in the CIS).

* MetaDataSync  (Brings over metadata from the CIS to the DAMS, Syncs the pathname in the CIS to point to the IDS derivative rather than the media drive).

* ThumbnailSync (replaces the thumbnail image in the CIS with the current thumbnail image in the DAMS system).

* Report        (generates and emails report showing the status/completion/errors of files through CDIS)


The java code for ALL of these execution types is found in this repository and is to be compiled into a single .jar file.


