package edu.arizona.cs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Main {

    private static final Log LOG = LogFactory.getLog(Main.class);
    
    private static final String[] packages = {"org.apache.hadoop.fs.hsynth.test"};
    
    public static void main(String[] args) throws Exception {
        if(args.length >= 1) {
            Class clazz = null;
            
            // check full package 
            String className = args[0];
            try {
                clazz = Class.forName(className);
            } catch(ClassNotFoundException ex) {
            }
        
            if(clazz == null) {
                for(String pkg : packages) {
                    String newClassName = pkg + "." + args[0];
                    try {
                        clazz = Class.forName(newClassName);
                    } catch(ClassNotFoundException ex) {
                    }

                    if(clazz != null) {
                        break;
                    }
                }
            }
            
            if(clazz == null) {
                LOG.error("Class was not found : " + args[0]);
                throw new Exception("Class was not found : " + args[0]);
            }
            
            // class found
            
            Method method = null;
            
            try {
                Class[] argTypes = new Class[] { String[].class };
                method = clazz.getDeclaredMethod("main", argTypes);
            } catch(NoSuchMethodException ex) {
                LOG.error("Main function was not found");
                throw new Exception(ex);
            }
            
            String[] newArgs = new String[args.length-1];
            System.arraycopy(args, 1, newArgs, 0, args.length-1);

            try {
                method.invoke(null, (Object)newArgs);
            } catch(InvocationTargetException ex) {
                LOG.error(ex.getCause());
                throw new Exception(ex.getCause());
            }
        } else {
            LOG.error("Invalid arguments were given - give class path of target class");
            throw new Exception("Invalid arguments were given - give class path of target class");
        }
    }
}
