/*
   Copyright 2015 The Trustees of Princeton University

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
    
    public static boolean timeElapsedSecond(long prev_time, long cur_time, long period_second) {
        long gap = period_second * 1000;
        if(cur_time - prev_time >= gap) {
            return true;
        }
        return false;
    }
}
