package sjdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Optimiser {
    private Catalogue catalogue;

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    public Operator optimise(Operator plan) {
        plan = transformToLeftDeepTree(plan);
        plan = pushdownSelections(plan);
        plan = combineToJoin(plan);
        plan = pushdownProjects(plan);
        return plan;
    }

    private Operator transformToLeftDeepTree(Operator plan) {
        if (plan instanceof Product) {
            // Use recursion to ensure that the left and right subtrees are both left-deep trees
            Operator left = transformToLeftDeepTree(((Product) plan).getLeft());
            Operator right = transformToLeftDeepTree(((Product) plan).getRight());

            // Reorganize the tree to be left-deep if the right subtree is a Product.
            if (right instanceof Product) {
                // Combine leftSubtree with the left child of the rightSubtree
                Operator newLeftSubtree = new Product(left, ((Product) right).getLeft());
                Operator newRightSubtree = ((Product) right).getRight();

                // Continue transforming the new left-deep subtree
                return new Product(transformToLeftDeepTree(newLeftSubtree), newRightSubtree);
            } else {
                // If the right side is not a Product operator, just reconnect it
                return new Product(left, right);
            }
        } else if (plan instanceof Select) {
            return new Select(transformToLeftDeepTree(((Select) plan).getInput()), ((Select) plan).getPredicate());
        } else if (plan instanceof Project) {
            return new Project(transformToLeftDeepTree(((Project) plan).getInput()), ((Project) plan).getAttributes());
        }
        // For the Scan operator, just return it.
        return plan;
    }

    private Operator pushdownSelections(Operator plan) {
        if (plan instanceof Select) {
            // Trying to push the Select operation further down the hierarchy
            Operator newInput = pushdownSelections(((Select) plan).getInput());
            // Check if the Select operation can be pushed down further
            return handleSelection(((Select) plan).getPredicate(), newInput);
        } else if (plan instanceof Project) {
            // For the Project operator, recursively process its child nodes
            Operator newInput = pushdownSelections(((UnaryOperator) plan).getInput());
            return new Project(newInput, ((Project) plan).getAttributes());
        } else if (plan instanceof Product) {
            // For the Product operator, two child nodes need to be handled
            Operator newLeft = pushdownSelections(((BinaryOperator) plan).getLeft());
            Operator newRight = pushdownSelections(((BinaryOperator) plan).getRight());
            return new Product(newLeft, newRight);
        }
        // For the Scan operator, just return it.
        return plan;
    }

    private Operator handleSelection(Predicate predicate, Operator input) {
        if (input instanceof Product) {
            BinaryOperator inputOperator = (BinaryOperator) input;
            // For the Product operator, the attributes involved in the predicate determine whether it can be pushed down or not
            if (canBePushedToLeftSubtree(predicate, input)) {
                // If the attributes are from the left subtree, then continue recursion to the left
                Operator left = pushdownSelections(new Select(inputOperator.getLeft(), predicate));
                return new Product(left, inputOperator.getRight());
            } else if (canBePushedToRightSubtree(predicate, input)) {
                // If the attributes are from the right subtree, then continue recursion to the right
                Operator right = pushdownSelections(new Select(inputOperator.getRight(), predicate));
                return new Product(inputOperator.getLeft(), right);
            }
        } else if (input instanceof UnaryOperator) {
            // Skip the UnaryOperator and try to push the selection down
            Operator pushed = handleSelection(predicate, ((UnaryOperator) input).getInput());

            if (input instanceof Project) {
                return new Project(pushed, ((Project) input).getAttributes());
            } else {
                return new Select(pushed, ((Select) input).getPredicate());
            }
        }
        // If the input of Select is Scan, just return the Select
        return new Select(input, predicate);
    }

    private boolean canBePushedToLeftSubtree(Predicate predicate, Operator operator) {
        if (!(operator instanceof BinaryOperator)) {
            return false;
        }
        Operator left = ((BinaryOperator) operator).getLeft();
        return containsAttribute(left, predicate.getLeftAttribute()) &&
                (predicate.getRightAttribute() == null || containsAttribute(left, predicate.getRightAttribute()));
    }

    private boolean canBePushedToRightSubtree(Predicate predicate, Operator operator) {
        if (!(operator instanceof BinaryOperator)) {
            return false;
        }
        Operator right = ((BinaryOperator) operator).getRight();
        return containsAttribute(right, predicate.getLeftAttribute()) &&
                (predicate.getRightAttribute() == null || containsAttribute(right, predicate.getRightAttribute()));
    }

    private boolean containsAttribute(Operator op, Attribute attr) {
        if (op instanceof BinaryOperator) {
            BinaryOperator binOp = (BinaryOperator) op;
            return containsAttribute(binOp.getLeft(), attr) || containsAttribute(binOp.getRight(), attr);
        }

        if (op instanceof UnaryOperator) {
            return containsAttribute(((UnaryOperator) op).getInput(), attr);
        }

        if (op.getOutput() != null) {
            for (Attribute outputAttr : op.getOutput().getAttributes()) {
                if (outputAttr.equals(attr)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Operator combineToJoin(Operator operator) {
        if (operator instanceof Select) {
            Predicate predicate = ((Select) operator).getPredicate();
            Operator childOperator = ((Select) operator).getInput();

            // If the child node is a Product operator, check if a Join operator can be formed.
            if (childOperator instanceof Product) {
                Operator leftChild = ((Product) childOperator).getLeft();
                Operator rightChild = ((Product) childOperator).getRight();
                if (canFormJoin(predicate, leftChild, rightChild)) {
                    return new Join(combineToJoin(leftChild),
                            combineToJoin(rightChild), predicate);
                }
            }
            return new Select(combineToJoin(childOperator), predicate);
        } else if (operator instanceof Project) {
            Operator child = ((UnaryOperator) operator).getInput();
            return new Project(combineToJoin(child), ((Project) operator).getAttributes());
        } else if (operator instanceof Product) {
            Operator left = combineToJoin(((BinaryOperator) operator).getLeft());
            Operator right = combineToJoin(((BinaryOperator) operator).getRight());
            return new Product(left, right);
        }

        // For the Scan operator, just return it.
        return operator;
    }

    private boolean canFormJoin(Predicate predicate, Operator leftChild, Operator rightChild) {
        boolean referencesLeft = referencesAttributes(predicate, leftChild);
        boolean referencesRight = referencesAttributes(predicate, rightChild);
        return referencesLeft && referencesRight;
    }

    private boolean referencesAttributes(Predicate predicate, Operator operator) {
        return containsAttribute(operator, predicate.getLeftAttribute()) ||
                (predicate.getRightAttribute() != null && containsAttribute(operator, predicate.getRightAttribute()));
    }

    private Operator pushdownProjects(Operator plan) {
        if (plan instanceof Project) {
            // Get the attributes needed by the top-level Project node
            Set<Attribute> requiredAttributes = new HashSet<>(((Project) plan).getAttributes());
            // Continue processing the operators below the Project while pushing down these attributes
            return new Project(pushdownProjectsRecursive(((Project) plan).getInput(), requiredAttributes), ((Project) plan).getAttributes());
        } else {
            // If the top-level node is not Project, it means that all attributes are required
            return pushdownProjectsRecursive(plan, new HashSet<>(getAllAttributes(plan)));
        }
    }

    private Operator pushdownProjectsRecursive(Operator operator, Set<Attribute> attributes) {
        // Set<Attribute> attributes is a reference type. A new Set object needs to be created in order not to affect the original attributes object.
        Set<Attribute> requiredAttributes = new HashSet<>(attributes);

        if (operator instanceof BinaryOperator) {
            Operator leftOp = ((BinaryOperator) operator).getLeft();
            Operator rightOp = ((BinaryOperator) operator).getRight();

            if (operator instanceof Join) {
                requiredAttributes.add(((Join) operator).getPredicate().getLeftAttribute());
                requiredAttributes.add(((Join) operator).getPredicate().getRightAttribute());
            }

            // For the BinaryOperator, we need to determine which attributes are needed for the left and right subtrees respectively
            Set<Attribute> leftAttrs = new HashSet<>();
            Set<Attribute> rightAttrs = new HashSet<>();
            for (Attribute attr : requiredAttributes) {
                if (containsAttribute(leftOp, attr)) {
                    leftAttrs.add(attr);
                }
                if (containsAttribute(rightOp, attr)) {
                    rightAttrs.add(attr);
                }
            }

            // Recursively processing subtrees
            Operator left = new Project(pushdownProjectsRecursive(leftOp, leftAttrs), new ArrayList<>(leftAttrs));
            Operator right = new Project(pushdownProjectsRecursive(rightOp, rightAttrs), new ArrayList<>(rightAttrs));

            if (operator instanceof Product) {
                return new Product(left, right);
            } else if (operator instanceof Join) {
                return new Join(left, right, ((Join) operator).getPredicate());
            }
        } else if (operator instanceof Project) {
            Operator input = pushdownProjectsRecursive(((UnaryOperator) operator).getInput(), requiredAttributes);
            return new Project(input, new ArrayList<>(requiredAttributes));
        } else if (operator instanceof Select) {
            Predicate predicate = ((Select) operator).getPredicate();
            requiredAttributes.add(predicate.getLeftAttribute());

            if (predicate.getRightAttribute() != null) {
                requiredAttributes.add(predicate.getRightAttribute());
            }
            Operator input = pushdownProjectsRecursive(((UnaryOperator) operator).getInput(), requiredAttributes);
            return new Select(input, predicate);
        }

        // For the Scan operator, just return it.
        return operator;
    }

    private Set<Attribute> getAllAttributes(Operator plan) {
        Set<Attribute> attributes = new HashSet<>();

        if (plan instanceof BinaryOperator) {
            attributes.addAll(getAllAttributes(((BinaryOperator) plan).getLeft()));
            attributes.addAll(getAllAttributes(((BinaryOperator) plan).getRight()));
        }

        if (plan instanceof Project) {
            attributes.addAll(((Project) plan).getAttributes());
        }

        if (plan instanceof Select) {
            attributes.addAll(getAllAttributes(((UnaryOperator) plan).getInput()));
        }

        if (plan instanceof Scan) {
            attributes.addAll(plan.getOutput().getAttributes());
        }

        return attributes;
    }
}
