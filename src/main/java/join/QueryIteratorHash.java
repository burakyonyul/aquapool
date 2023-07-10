package join;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIteratorBase;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.util.Utils;
import org.apache.jena.atlas.io.IndentedWriter;

import java.util.Iterator;
import java.util.List;

public class QueryIteratorHash extends QueryIteratorBase {
    private Iterator<Binding> bindingIter;

    public QueryIteratorHash(List<Binding> bindingList) {
        this.bindingIter = bindingList.iterator();
    }

    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.print(Utils.className(this));
    }

    protected boolean hasNextBinding() {
        return this.bindingIter.hasNext();
    }

    protected Binding moveToNextBinding() {
        return (Binding)this.bindingIter.next();
    }

    protected void closeIterator() {
        this.bindingIter = null;
    }

    protected void requestCancel() {
    }
}
