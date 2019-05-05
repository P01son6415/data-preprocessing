package word2vec;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class SyncFileWriter {

    private FileWriter fw;
    private PrintWriter pw;

    public SyncFileWriter(String filepath) throws IOException {
        this.fw = new FileWriter(filepath, true);
        this.pw = new PrintWriter(fw);
    }
    public void writeAndFlush(String text) throws IOException{
        synchronized (this){
            fw.write(text);
            fw.flush();
        }
    }

}
