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
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation inputRelation = op.getInput().getOutput();
		Relation outputRelation = new Relation(inputRelation.getTupleCount());
		List<Attribute> projectAttributes = op.getAttributes();

		for (Attribute attr : projectAttributes) {
			if (inputRelation.getAttributes().contains(attr)) {
				outputRelation.addAttribute(new Attribute(attr.getName(), inputRelation.getAttribute(attr).getValueCount()));
			}
		}

		op.setOutput(outputRelation);
	}


	public void visit(Select op) {
		Relation inputRelation = op.getInput().getOutput();
		Predicate predicate = op.getPredicate();
		int estimatedTuples;
		Relation outputRelation;

		if (predicate.equalsValue()) {
			String attrName = predicate.getLeftAttribute().getName();
			Attribute inputAttribute = inputRelation.getAttribute(new Attribute(attrName));

			estimatedTuples = inputRelation.getTupleCount() / inputAttribute.getValueCount();
			outputRelation = new Relation(estimatedTuples);

			for (Attribute attr : inputRelation.getAttributes()) {
				if (attr.getName().equals(attrName)) {
					outputRelation.addAttribute(new Attribute(attr.getName(), 1));
				} else {
					outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount()));
				}
			}
		} else {
			Attribute leftAttr = inputRelation.getAttribute(predicate.getLeftAttribute());
			Attribute rightAttr = inputRelation.getAttribute(predicate.getRightAttribute());
			int maxDistinctValues = Math.max(leftAttr.getValueCount(), rightAttr.getValueCount());
			int minDistinctValues = Math.min(leftAttr.getValueCount(), rightAttr.getValueCount());

			estimatedTuples = inputRelation.getTupleCount() / maxDistinctValues;
			outputRelation = new Relation(estimatedTuples);

			for (Attribute attr : inputRelation.getAttributes()) {
				if (attr.getName().equals(leftAttr.getName()) || attr.getName().equals(rightAttr.getName())) {
					outputRelation.addAttribute(new Attribute(attr.getName(), minDistinctValues));
				} else {
					outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount()));
				}
			}
		}

		op.setOutput(outputRelation);
	}



	public void visit(Product op) {
		Relation leftRelation = op.getLeft().getOutput();
		Relation rightRelation = op.getRight().getOutput();
		int outputTupleCount = leftRelation.getTupleCount() * rightRelation.getTupleCount();

		Relation outputRelation = new Relation(outputTupleCount);
		for (Attribute attr : leftRelation.getAttributes()) {
			outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount()));
		}
		for (Attribute attr : rightRelation.getAttributes()) {
			outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount()));
		}

		op.setOutput(outputRelation);
	}


	public void visit(Join op) {
		Relation leftRelation = op.getLeft().getOutput();
		Relation rightRelation = op.getRight().getOutput();
		Predicate predicate = op.getPredicate();

		Attribute leftAttribute = leftRelation.getAttribute(predicate.getLeftAttribute());
		Attribute rightAttribute = rightRelation.getAttribute(predicate.getRightAttribute());
		int maxDistinctValues = Math.max(leftAttribute.getValueCount(), rightAttribute.getValueCount());
		int minDistinctValues = Math.min(leftAttribute.getValueCount(), rightAttribute.getValueCount());

		int estimatedTuples = (leftRelation.getTupleCount() * rightRelation.getTupleCount()) / maxDistinctValues;

		Relation outputRelation = new Relation(estimatedTuples);
		for (Attribute attr : leftRelation.getAttributes()) {
			if (attr.getName().equals(leftAttribute.getName())) {
				outputRelation.addAttribute(new Attribute(attr.getName(), minDistinctValues));
			} else {
				outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount()));
			}
		}
		for (Attribute attr : rightRelation.getAttributes()) {
			if (attr.getName().equals(rightAttribute.getName())) {
				continue;
			}
			outputRelation.addAttribute(new Attribute(attr.getName(), attr.getValueCount()));
		}

		op.setOutput(outputRelation);
	}
}
