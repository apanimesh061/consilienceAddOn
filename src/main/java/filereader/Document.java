package filereader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.elasticsearch.common.Base64;

/**
 * Created by cloudera on 7/15/15.
 */

public class Document {

    private int docType;
    private String content;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getDocType() {
        return docType;
    }

    public void setDocType(int docType) {
        this.docType = docType;
    }

    Document(int docType) {
        if(docType == 0) {
        }
    }

    public String readPlainText() throws IOException {
        //data will come from MongoDB
        return null;
    }

    public String encode(byte[] data) throws IOException {
        return Base64.encodeBytes(data);
    }

    public String readNonPDFType(File file) throws IOException {
        byte[] encoded = Files.readAllBytes(file.toPath());
        return new String(encoded, "utf-8");
    }
}
