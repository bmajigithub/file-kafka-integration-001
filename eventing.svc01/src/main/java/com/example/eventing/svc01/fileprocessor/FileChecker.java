package com.example.eventing.svc01.fileprocessor;

import java.io.File;

public class FileChecker {

    private String fname;
    private File f;

    public FileChecker(String fname) {
        this.fname = fname;
        this.f = new File(fname);
    }

    public boolean fileExists() {
        return f.exists();
    }
    public String getFileName() {
        return f.getName();
    }
    public void deleteFile() {
        f.delete();
    }

}
