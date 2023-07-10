package join;

import com.hp.hpl.jena.graph.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ComplexNode {
    private List<Node> nodes;

    public ComplexNode() {
        this.nodes = new ArrayList();
    }

    public ComplexNode(List<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Node> getNodes() {
        return this.nodes;
    }

    public void add(Node node) {
        if (!this.nodes.contains(node)) {
            this.nodes.add(node);
        }

    }

    public int hashCode() {
        int finalHash = 1;
        if (this.nodes == null) {
            return 0;
        } else {
            Node node;
            for (Iterator var2 = this.nodes.iterator(); var2.hasNext(); finalHash *= node.hashCode()) {
                node = (Node) var2.next();
            }

            return finalHash;
        }
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            ComplexNode other = (ComplexNode) obj;
            if (this.nodes == null) {
                if (other.nodes != null) {
                    return false;
                }
            } else {
                Iterator var3 = this.nodes.iterator();

                while (var3.hasNext()) {
                    Node node = (Node) var3.next();
                    if (!other.nodes.contains(node)) {
                        return false;
                    }
                }
            }

            return true;
        }
    }
}

