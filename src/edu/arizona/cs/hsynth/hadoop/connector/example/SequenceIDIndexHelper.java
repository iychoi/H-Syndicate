package edu.arizona.cs.hsynth.hadoop.connector.example;

import edu.arizona.cs.hsynth.fs.HSynthFSConfiguration;
import edu.arizona.cs.hsynth.fs.HSynthFSPath;
import edu.arizona.cs.hsynth.fs.HSynthFSPathFilter;
import edu.arizona.cs.hsynth.fs.HSynthFileSystem;
import edu.arizona.cs.hsynth.hadoop.util.HSynthConfigUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class SequenceIDIndexHelper {
    
    public static String[] getIndexDataFiles(Configuration conf, String comma_separated_input) throws IOException {
        String[] inputs = splitCommaSeparated(comma_separated_input);
        return getIndexDataFiles(conf, inputs);
    }
    
    public static String[] getIndexDataFiles(Configuration conf, String[] inputs) throws IOException {
        HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration hsynthconf = HSynthConfigUtil.getHSynthFSConfigurationInstance(conf);
            fs = hsynthconf.getContext().getFileSystem();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        List<String> indices = new ArrayList<String>();
        
        for(String input : inputs) {
            HSynthFSPath inputPath = new HSynthFSPath(input);

            String filename = SequenceIDIndexHelper.generateNamedOutput(inputPath);

            HSynthFSPath[] files = fs.listAllFiles(inputPath.getParent());
            for (HSynthFSPath f : files) {
                if (fs.isDirectory(f)) {
                    if (f.getName().startsWith(filename + "-r-")) {
                        // found
                        indices.add(f.getPath() + "/data");
                    }
                }
            }
        }
        
        String[] paths = new String[indices.size()];
        paths = indices.toArray(paths);

        return paths;
    }
    
    public static String[] getIndexFiles(Configuration conf, String comma_separated_input) throws IOException {
        String[] inputs = splitCommaSeparated(comma_separated_input);
        return getIndexFiles(conf, inputs);
    }
    
    public static String[] getIndexFiles(Configuration conf, String[] inputs) throws IOException {
        HSynthFileSystem fs = null;
        try {
            HSynthFSConfiguration hsynthconf = HSynthConfigUtil.getHSynthFSConfigurationInstance(conf);
            fs = hsynthconf.getContext().getFileSystem();
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        }
        List<String> indices = new ArrayList<String>();
        
        for(String input : inputs) {
            HSynthFSPath inputPath = new HSynthFSPath(input);

            String filename = SequenceIDIndexHelper.generateNamedOutput(inputPath);

            HSynthFSPath[] files = fs.listAllFiles(inputPath.getParent());
            for (HSynthFSPath f : files) {
                if (fs.isDirectory(f)) {
                    if (f.getName().startsWith(filename + "-r-")) {
                        // found
                        indices.add(f.getPath());
                    }
                }
            }
        }
        
        String[] paths = new String[indices.size()];
        paths = indices.toArray(paths);

        return paths;
    }
    
    public static String makeCommaSeparated(HSynthFSPath[] strs) {
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
    
    public static HSynthFSPath[] getAllInputPaths(HSynthFileSystem fs, String[] inputPaths, HSynthFSPathFilter filter) throws IOException {
        List<HSynthFSPath> inputFiles = new ArrayList<HSynthFSPath>();
        
        for(String path : inputPaths) {
            inputFiles.add(new HSynthFSPath(path));
        }
        
        HSynthFSPath[] files = new HSynthFSPath[inputFiles.size()];
        files = inputFiles.toArray(files);
        return getAllInputPaths(fs, files, filter);
    }
    
    public static HSynthFSPath[] getAllInputPaths(HSynthFileSystem fs, HSynthFSPath[] inputPaths, HSynthFSPathFilter filter) throws IOException {
        List<HSynthFSPath> inputFiles = new ArrayList<HSynthFSPath>();
        
        for(HSynthFSPath path : inputPaths) {
            if(fs.isDirectory(path)) {
                HSynthFSPath[] entries = fs.listAllFiles(path);
                for(HSynthFSPath entry : entries) {
                    if(filter.accept(entry)) {
                        inputFiles.add(entry);
                    }
                }
            } else {
                if(filter.accept(path)) {
                    inputFiles.add(path);
                }
            }
        }
        
        HSynthFSPath[] files = new HSynthFSPath[inputFiles.size()];
        files = inputFiles.toArray(files);
        return files;
    }
    
    public static String[] generateNamedOutputs(HSynthFSPath[] inputPaths) {
        String[] namedOutputs = new String[inputPaths.length];
        for(int i=0;i<inputPaths.length;i++) {
            namedOutputs[i] = generateNamedOutput(inputPaths[i]);
        }
        return namedOutputs;
    }
    
    public static String generateNamedOutput(HSynthFSPath inputPath) {
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
