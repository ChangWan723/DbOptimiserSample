package sjdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Optimiser {
    private Catalogue catalogue;

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    // using the Heuristics method
    // comments were added at crucial points in the code to help understanding
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

    // Recursively determine if the operator contains the attr
    private boolean containsAttribute(Operator operator, Attribute attr) {
        if (operator instanceof BinaryOperator) {
            BinaryOperator binOp = (BinaryOperator) operator;
            return containsAttribute(binOp.getLeft(), attr) || containsAttribute(binOp.getRight(), attr);
        }

        if (operator instanceof UnaryOperator) {
            return containsAttribute(((UnaryOperator) operator).getInput(), attr);
        }

        if (operator.getOutput() != null) {
            for (Attribute outputAttr : operator.getOutput().getAttributes()) {
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
        boolean isReferencesLeft = isReferencesAttributes(predicate, leftChild);
        boolean isReferencesRight = isReferencesAttributes(predicate, rightChild);
        return isReferencesLeft && isReferencesRight;
    }

    // Determine if attributes in predicate are referenced (contained) in operator
    private boolean isReferencesAttributes(Predicate predicate, Operator operator) {
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
            return pushdownProjectsRecursive(plan, new HashSet<>(getAllProjectAttributes(plan)));
        }
    }

    private Operator pushdownProjectsRecursive(Operator operator, Set<Attribute> attributes) {
        // Set<Attribute> attributes is a reference type.
        // So, a new Set object needs to be created in order not to affect the original attributes object.
        Set<Attribute> requiredAttributes = new HashSet<>(attributes);

        if (operator instanceof BinaryOperator) {
            Operator leftOp = ((BinaryOperator) operator).getLeft();
            Operator rightOp = ((BinaryOperator) operator).getRight();

            // For Join operator, we need to add the attributes of predicate to the requiredAttributes
            if (operator instanceof Join) {
                requiredAttributes.add(((Join) operator).getPredicate().getLeftAttribute());
                requiredAttributes.add(((Join) operator).getPredicate().getRightAttribute());
            }

            // For the BinaryOperator, we need to split the requiredAttributes into left and right subtrees
            Set<Attribute> leftRequiredAttrs = new HashSet<>();
            Set<Attribute> rightRequiredAttrs = new HashSet<>();
            requiredAttributes.forEach(attr -> {
                if (containsAttribute(leftOp, attr)) {
                    leftRequiredAttrs.add(attr);
                }
                if (containsAttribute(rightOp, attr)) {
                    rightRequiredAttrs.add(attr);
                }
            });

            // Create new Project() for the left and right subtrees respectively, and continue the recursion
            Operator left = new Project(pushdownProjectsRecursive(leftOp, leftRequiredAttrs), new ArrayList<>(leftRequiredAttrs));
            Operator right = new Project(pushdownProjectsRecursive(rightOp, rightRequiredAttrs), new ArrayList<>(rightRequiredAttrs));

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

            // For Select operator, we need to add the attributes of predicate to the requiredAttributes
            if (predicate.getRightAttribute() != null) {
                requiredAttributes.add(predicate.getRightAttribute());
            }
            Operator input = pushdownProjectsRecursive(((UnaryOperator) operator).getInput(), requiredAttributes);
            return new Select(input, predicate);
        }

        // For the Scan operator, just return it.
        return operator;
    }

    // We can't guarantee that getOutput() is not null except for Scan
    // So we need to recursively get all attributes that need to be projected
    private Set<Attribute> getAllProjectAttributes(Operator plan) {
        Set<Attribute> attributes = new HashSet<>();

        if (plan instanceof BinaryOperator) {
            attributes.addAll(getAllProjectAttributes(((BinaryOperator) plan).getLeft()));
            attributes.addAll(getAllProjectAttributes(((BinaryOperator) plan).getRight()));
        }

        // For Project operator, there is no need for recursion, just add getAttributes()
        if (plan instanceof Project) {
            attributes.addAll(((Project) plan).getAttributes());
        }

        if (plan instanceof Select) {
            attributes.addAll(getAllProjectAttributes(((UnaryOperator) plan).getInput()));
        }

        if (plan instanceof Scan) {
            attributes.addAll(plan.getOutput().getAttributes());
        }

        return attributes;
    }
}
