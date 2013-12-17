package edu.arizona.cs.hsynth.hadoop.example.filesystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class SequenceIDIndexHelper {
    
    public static String[] getIndexDataFiles(Configuration conf, String comma_separated_input) throws IOException {
        String[] inputs = splitCommaSeparated(comma_separated_input);
        return getIndexDataFiles(conf, inputs);
    }
    
    public static String[] getIndexDataFiles(Configuration conf, String[] inputs) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        List<String> indices = new ArrayList<String>();
        
        for(String input : inputs) {
            Path inputPath = new Path(input);

            String filename = SequenceIDIndexHelper.generateNamedOutput(inputPath);

            FileStatus[] files = fs.listStatus(inputPath.getParent());
            for (FileStatus status : files) {
                if (status.isDir()) {
                    if (status.getPath().getName().startsWith(filename + "-r-")) {
                        // found
                        indices.add(status.getPath().toString() + "/data");
                    }
                }
            }
        }
        
        //String[] paths = new String[indices.size()];
        //paths = indices.toArray(paths);
        String[] paths = indices.toArray(new String[0]);

        return paths;
    }
    
    public static String[] getIndexFiles(Configuration conf, String comma_separated_input) throws IOException {
        String[] inputs = splitCommaSeparated(comma_separated_input);
        return getIndexFiles(conf, inputs);
    }
    
    public static String[] getIndexFiles(Configuration conf, String[] inputs) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        List<String> indices = new ArrayList<String>();
        
        for(String input : inputs) {
            Path inputPath = new Path(input);

            String filename = SequenceIDIndexHelper.generateNamedOutput(inputPath);

            FileStatus[] files = fs.listStatus(inputPath.getParent());
            for (FileStatus status : files) {
                if (status.isDir()) {
                    if (status.getPath().getName().startsWith(filename + "-r-")) {
                        // found
                        indices.add(status.getPath().toString());
                    }
                }
            }
        }
        
        //String[] paths = new String[indices.size()];
        //paths = indices.toArray(paths);
        String[] paths = indices.toArray(new String[0]);

        return paths;
    }
    
    public static String makeCommaSeparated(Path[] strs) {
        if(strs == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<strs.length;i++) {
            sb.append(strs[i].toString());
            if(i < strs.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    public static String makeCommaSeparated(String[] strs) {
        if(strs == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<strs.length;i++) {
            sb.append(strs[i]);
            if(i < strs.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    public static String[] splitCommaSeparated(String comma_separated_input) {
        String[] inputs = comma_separated_input.split(",");
        return inputs;
    }
    
    public static String getSafeNamedOutput(String input) {
        StringBuffer sb = new StringBuffer();
        
        for (char ch : input.toCharArray()) {
            boolean isSafe = false;
            if ((ch >= 'A') && (ch <= 'Z')) {
                isSafe = true;
            } else if ((ch >= 'a') && (ch <= 'z')) {
                isSafe = true;
            } else if ((ch >= '0') && (ch <= '9')) {
                isSafe = true;
            }
            
            if(isSafe) {
                sb.append(ch);
            }
        }
        
        sb.append(FastaSequenceIDConstants.NAMED_OUTPUT_NAME_SURFIX);
        
        return sb.toString();
    }
    
    public static Path[] getAllInputPaths(FileSystem fs, String[] inputPaths, PathFilter filter) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        
        for(String path : inputPaths) {
            inputFiles.add(new Path(path));
        }
        
        //Path[] files = new Path[inputFiles.size()];
        //files = inputFiles.toArray(files);
        Path[] files = inputFiles.toArray(new Path[0]);
        return getAllInputPaths(fs, files, filter);
    }
    
    public static Path[] getAllInputPaths(FileSystem fs, Path[] inputPaths, PathFilter filter) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        
        for(Path path : inputPaths) {
            FileStatus status = fs.getFileStatus(path);
            if(status.isDir()) {
                FileStatus[] entries = fs.listStatus(path);
                for(FileStatus entry : entries) {
                    if(filter.accept(entry.getPath())) {
                        inputFiles.add(entry.getPath());
                    }
                }
            } else {
                if(filter.accept(path)) {
                    inputFiles.add(path);
                }
            }
        }
        
        //Path[] files = new Path[inputFiles.size()];
        //files = inputFiles.toArray(files);
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static String[] generateNamedOutputs(Path[] inputPaths) {
        String[] namedOutputs = new String[inputPaths.length];
        for(int i=0;i<inputPaths.length;i++) {
            namedOutputs[i] = generateNamedOutput(inputPaths[i]);
        }
        return namedOutputs;
    }
    
    public static String generateNamedOutput(Path inputPath) {
        return getSafeNamedOutput(inputPath.getName());
    }
    
    public static String generateNamedOutput(String inputPath) {
        int lastDir = inputPath.lastIndexOf("/");
        if(lastDir >= 0) {
            return getSafeNamedOutput(inputPath.substring(lastDir + 1));
        }
        return getSafeNamedOutput(inputPath);
    }
}
