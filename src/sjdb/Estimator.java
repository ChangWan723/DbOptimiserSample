package sjdb;

import java.util.List;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


    public Estimator() {
        // empty constructor
    }

    /*
     * Create output relation on Scan operator
     *
     * Example implementation of visit method for Scan operators.
     */
    public void visit(Scan op) {
        Relation inputRelation = op.getRelation();
        Relation outputRelation = new Relation(inputRelation.getTupleCount());

        inputRelation.getAttributes().forEach(attribute -> outputRelation.addAttribute(new Attribute(attribute)));

        op.setOutput(outputRelation);
    }

    public void visit(Project op) {
        Relation inputRelation = op.getInput().getOutput();
        Relation outputRelation = new Relation(inputRelation.getTupleCount());

        op.getAttributes().stream()
                .filter(attr -> inputRelation.getAttributes().contains(attr))
                .forEach(attr -> outputRelation.addAttribute(new Attribute(attr.getName(), inputRelation.getAttribute(attr).getValueCount())));

        op.setOutput(outputRelation);
    }

    public void visit(Select op) {
        Relation inputRelation = op.getInput().getOutput();
        Predicate predicate = op.getPredicate();
        Relation outputRelation;

        if (predicate.equalsValue()) {
            String predicateAttrName = predicate.getLeftAttribute().getName();
            Attribute inputAttribute = inputRelation.getAttribute(new Attribute(predicateAttrName));

            int estimatedTuples = inputRelation.getTupleCount() / inputAttribute.getValueCount();
            outputRelation = new Relation(estimatedTuples);

            inputRelation.getAttributes()
                    .forEach(attr -> {
                        int valueCount = isSameAttr(predicateAttrName, attr) ? 1 : attr.getValueCount();
                        outputRelation.addAttribute(new Attribute(attr.getName(), valueCount));
                    });
        } else {
            Attribute leftAttr = inputRelation.getAttribute(predicate.getLeftAttribute());
            Attribute rightAttr = inputRelation.getAttribute(predicate.getRightAttribute());

            int maxAttrValues = Math.max(leftAttr.getValueCount(), rightAttr.getValueCount());
            int estimatedTuples = inputRelation.getTupleCount() / maxAttrValues;
            outputRelation = new Relation(estimatedTuples);

            int minAttrValues = Math.min(leftAttr.getValueCount(), rightAttr.getValueCount());
            inputRelation.getAttributes()
                    .forEach(attr -> {
                        int valueCount = isSameAttr(leftAttr.getName(), attr) || isSameAttr(rightAttr.getName(), attr)
                                ? minAttrValues : attr.getValueCount();
                        outputRelation.addAttribute(new Attribute(attr.getName(), valueCount));
                    });
        }

        op.setOutput(outputRelation);
    }

    private static boolean isSameAttr(String attrName, Attribute attr) {
        return attr.getName().equals(attrName);
    }

    public void visit(Product op) {
        Relation leftRelation = op.getLeft().getOutput();
        Relation rightRelation = op.getRight().getOutput();

        int estimatedTuples = leftRelation.getTupleCount() * rightRelation.getTupleCount();
        Relation outputRelation = new Relation(estimatedTuples);

        leftRelation.getAttributes()
                .forEach(attr -> outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount())));
        rightRelation.getAttributes()
                .forEach(attr -> outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount())));

        op.setOutput(outputRelation);
    }

    public void visit(Join op) {
        Relation leftRelation = op.getLeft().getOutput();
        Relation rightRelation = op.getRight().getOutput();
        Predicate predicate = op.getPredicate();

        Attribute leftAttribute;
        Attribute rightAttribute;
        // for the Join predicate, the leftAttribute is not always in the leftRelation  (it could be rightRelation),
        // so, the judgement is required
        if (leftRelation.getAttributes().contains(predicate.getLeftAttribute())) {
            leftAttribute = leftRelation.getAttribute(predicate.getLeftAttribute());
            rightAttribute = rightRelation.getAttribute(predicate.getRightAttribute());
        } else {
            leftAttribute = rightRelation.getAttribute(predicate.getLeftAttribute());
            rightAttribute = leftRelation.getAttribute(predicate.getRightAttribute());
        }

        int maxAttrValues = Math.max(leftAttribute.getValueCount(), rightAttribute.getValueCount());
        int estimatedTuples = (leftRelation.getTupleCount() * rightRelation.getTupleCount()) / maxAttrValues;
        Relation outputRelation = new Relation(estimatedTuples);

        int minAttrValues = Math.min(leftAttribute.getValueCount(), rightAttribute.getValueCount());
        leftRelation.getAttributes().forEach(attr -> {
            int valueCount = isSameAttr(leftAttribute.getName(), attr) || isSameAttr(rightAttribute.getName(), attr)
                    ? minAttrValues : attr.getValueCount();
            outputRelation.addAttribute(new Attribute(attr.getName(), valueCount));
        });
        rightRelation.getAttributes().forEach(attr -> {
            int valueCount = isSameAttr(leftAttribute.getName(), attr) || isSameAttr(rightAttribute.getName(), attr)
                    ? minAttrValues : attr.getValueCount();
            outputRelation.addAttribute(new Attribute(attr.getName(), valueCount));
        });

        op.setOutput(outputRelation);
    }
}
