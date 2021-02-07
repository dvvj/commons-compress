package org.apache.commons.compress.my;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;

public class ArchiveStreamFactory_Inspect {
  private static void testReadEntries(String testFile, int chunkSize) throws Exception {

    int chunkCount = 1;

    int fromEntry = 0;

    while (true) {
      System.out.println("========================= chunk count: " + chunkCount);
      int size = chunkSize * chunkCount;
      byte[] bytes = new byte[size];
      InputStream fileStream = new FileInputStream(testFile);
      int sizeRead = fileStream.read(bytes, 0, size);
      fileStream.close();
      System.out.println(String.format("chunk count: %d, size read: %d", chunkCount, sizeRead));

      ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes, 0, sizeRead);
      int entriesRead = 0;
      ArchiveInputStream archiveStream = null;
      try {
//        archiveStream = getArchiveInputStreamForCommonZip(byteStream);

        archiveStream = getArchiveInputStreamForTgz(byteStream);

        int entriesToSkip = fromEntry;
        while (entriesToSkip > 0) {
          archiveStream.getNextEntry();
          entriesToSkip --;
        }

        while (true) {
          ArchiveEntry entry = archiveStream.getNextEntry();
          if (entry == null) {
            break;
          }
          entriesRead ++;
          System.out.println(
            String.format("\tentry: %s, entry size: %d", entry.getName(), entry.getSize())
          );
        }

      } catch (Exception ex) {
        if (archiveStream != null) {
          System.out.println(
            String.format("stream status: getBytesRead=%d", archiveStream.getBytesRead())
          );
        }
        ex.printStackTrace();
        System.out.println("Exception: " + ex.getMessage());
      }

      System.out.println(
        String.format("\t** total entries: %d", entriesRead)
      );
      fromEntry += entriesRead;
      byteStream.close();

      if (sizeRead < size) {
        break;
      }

      chunkCount ++;
    }

  }

  private static ArchiveInputStream getArchiveInputStreamForCommonZip(InputStream byteStream) throws Exception {
    return new ArchiveStreamFactory().createArchiveInputStream(byteStream);
  }

  private static ArchiveInputStream getArchiveInputStreamForTgz(InputStream byteStream) throws Exception {
    InputStream gzi = new GzipCompressorInputStream(byteStream);
    return new TarArchiveInputStream(gzi);
  }

  public static void main(String[] args) throws Exception {
    String testFile = "/media/sf_vmshare/" +
      //"affextracts.zip";
      "test2.tar.gz";

    testReadEntries(testFile, 1024*500);

  }
}
