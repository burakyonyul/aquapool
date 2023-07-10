package join;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ResultSetMerger {
    public ResultSetMerger() {
    }

    public ResultSet mergeResultSets(ResultSet firstResultSet, ResultSet secondResultSet) throws Exception {
        List<String> firstResultVars = firstResultSet.getResultVars();
        List<String> secondResultVars = secondResultSet.getResultVars();
        List<Var> commonVars = this.findCommonVariables(firstResultVars, secondResultVars);
        HashMap<Integer, List<Binding>> firstMap = this.constructMap(firstResultSet, commonVars);
        HashMap<Integer, List<Binding>> secondMap = this.constructMap(secondResultSet, commonVars);
        Set<Integer> keys = firstMap.keySet();
        List<Binding> finalBindings = new ArrayList();
        Iterator var10 = keys.iterator();

        while (var10.hasNext()) {
            Integer key = (Integer) var10.next();
            List<Binding> firstBindings = (List) firstMap.get(key);
            List<Binding> secondBindings = (List) secondMap.get(key);
            if (firstBindings != null && secondBindings != null) {
                this.mergeBindings(commonVars, finalBindings, firstBindings, secondBindings);
            }
        }

        QueryIteratorHash queryIteratorHash = new QueryIteratorHash(finalBindings);
        List<String> variables = this.mergeAllVariables(firstResultVars, secondResultVars);
        ResultSet resultSetFinal = ResultSetFactory.create(queryIteratorHash, variables);
        return resultSetFinal;
    }

    private List<String> mergeAllVariables(List<String> firstResultVars, List<String> secondResultVars) {
        List<String> mergedVars = new ArrayList();
        this.insertVariables(firstResultVars, mergedVars);
        this.insertVariables(secondResultVars, mergedVars);
        return mergedVars;
    }

    private void insertVariables(List<String> resultVars, List<String> mergedVars) {
        Iterator var3 = resultVars.iterator();

        while (var3.hasNext()) {
            String var = (String) var3.next();
            if (!mergedVars.contains(var)) {
                mergedVars.add(var);
            }
        }

    }

    private void mergeBindings(List<Var> commonVars, List<Binding> finalBindings, List<Binding> firstBindings, List<Binding> secondBindings) {
        Iterator var5 = firstBindings.iterator();

        while (var5.hasNext()) {
            Binding firstBinding = (Binding) var5.next();
            Iterator var7 = secondBindings.iterator();

            while (var7.hasNext()) {
                Binding secondBinding = (Binding) var7.next();
                BindingHashMap bindingHashMap = this.mergeTwoBindings(commonVars, firstBinding, secondBinding);
                finalBindings.add(bindingHashMap);
            }
        }

    }

    private BindingHashMap mergeTwoBindings(List<Var> commonVars, Binding firstBinding, Binding secondBinding) {
        BindingHashMap bindingHashMap = new BindingHashMap();
        bindingHashMap.addAll(secondBinding);
        this.addUncommonMappings(commonVars, firstBinding, bindingHashMap);
        return bindingHashMap;
    }

    private void addUncommonMappings(List<Var> commonVars, Binding firstBinding, BindingHashMap bindingHashMap) {
        Iterator varIter = firstBinding.vars();

        while (varIter.hasNext()) {
            Var var = (Var) varIter.next();
            if (!commonVars.contains(var)) {
                bindingHashMap.add(var, firstBinding.get(var));
            }
        }

    }

    private HashMap<Integer, List<Binding>> constructMap(ResultSet resultSet, List<Var> commonVars) {
        HashMap map = new HashMap();

        while (resultSet.hasNext()) {
            Binding binding = resultSet.nextBinding();
            ComplexNode complexNode = new ComplexNode();
            Iterator var6 = commonVars.iterator();

            while (var6.hasNext()) {
                Var var = (Var) var6.next();
                Node node = binding.get(var);
                complexNode.add(node);
            }

            List<Binding> bindings = (List) map.get(complexNode.hashCode());
            if (bindings == null) {
                bindings = new ArrayList();
            }

            ((List) bindings).add(binding);
            map.put(complexNode.hashCode(), bindings);
        }

        return map;
    }

    private List<Var> findCommonVariables(List<String> firstResultVars, List<String> secondResultVars) {
        List<Var> commonVars = new ArrayList();
        Iterator var4 = firstResultVars.iterator();

        while (var4.hasNext()) {
            String firstVar = (String) var4.next();
            if (secondResultVars.contains(firstVar)) {
                commonVars.add(Var.alloc(firstVar));
            }
        }

        return commonVars;
    }
}
