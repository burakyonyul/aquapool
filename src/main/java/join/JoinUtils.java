package join;

import java.util.Iterator;
import java.util.List;

public class JoinUtils {
    public static boolean matchAnyVar(List<String> firstVars, List<String> secondVars) {

        for (String firstVar : firstVars) {

            for (String secondVar : secondVars) {
                if (firstVar.equals(secondVar)) {
                    return true;
                }
            }
        }

        return false;
    }
}
