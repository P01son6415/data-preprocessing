package word2vec;

import java.io.BufferedInputStream;
import java.io.*;
import java.util.Calendar;

public class TestRowCount {
    public static void main(String[] args) {
        long datestart = Calendar.getInstance().getTimeInMillis();
        int count = getFileLineCounts("XXX");
        System.out.println(count);
        long dateend = Calendar.getInstance().getTimeInMillis();
        System.out.println((dateend - datestart) / 1000);
    }

    public static int getFileLineCount(String filename) {
        int cnt = 0;
        LineNumberReader reader = null;
        try {
            reader = new LineNumberReader(new FileReader(filename));
            @SuppressWarnings("unused")
            String lineRead = "";
            while ((lineRead = reader.readLine()) != null) {
            }
            cnt = reader.getLineNumber();
        } catch (Exception ex) {
            cnt = -1;
            ex.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return cnt;
    }

    public static int getFileLineCounts(String filename) {
        int cnt = 0;
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(filename));
            byte[] c = new byte[1024];
            int readChars = 0;
            while ((readChars = is.read(c)) != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++cnt;
                    }
                }
            }
        } catch (Exception ex) {
            cnt = -1;
            ex.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return cnt;
    }
}