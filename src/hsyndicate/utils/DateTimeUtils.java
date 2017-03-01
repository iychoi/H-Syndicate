/*
   Copyright 2016 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package hsyndicate.utils;

public class DateTimeUtils {
    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }
    
    public static boolean timeElapsedSecond(long prevTime, long curTime, long periodSecond) {
        long gap = periodSecond * 1000;
        if(curTime - prevTime >= gap) {
            return true;
        }
        return false;
    }
}
