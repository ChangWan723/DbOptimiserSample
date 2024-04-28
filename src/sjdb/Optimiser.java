package sjdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Optimiser {
    private Catalogue catalogue;

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    public Operator optimise(Operator plan) {
        Estimator est = new Estimator();

        plan.accept(est);
        plan = transformToLeftDeepTree(plan);

        plan.accept(est);
        plan = pushdownSelections(plan);

        plan.accept(est);
        plan = combineToJoin(plan);

        plan.accept(est);
        plan = pushdownProjections(plan);

        return plan;
    }


    private Operator transformToLeftDeepTree(Operator plan) {
        if (plan instanceof Product) {
            BinaryOperator binaryOp = (BinaryOperator) plan;

            // 递归确保左侧和右侧子树都是左深树
            Operator left = transformToLeftDeepTree(binaryOp.getLeft());
            Operator right = transformToLeftDeepTree(binaryOp.getRight());

            // 如果右侧是Product操作符，进行重组以形成左深树。
            if (right instanceof Product) {
                // 将当前节点的右子节点的左子节点与当前节点的左子节点连接
                Operator newLeft = new Product(left, ((BinaryOperator) right).getLeft());

                // 继续处理这种新的左深结构
                return new Product(transformToLeftDeepTree(newLeft), ((BinaryOperator) right).getRight());
            } else {
                // 如果右侧不是二元操作符，只需重新连接即可
                return new Product(left, right);
            }
        } else if (plan instanceof Select) {
            return new Select(transformToLeftDeepTree(((UnaryOperator) plan).getInput()), ((Select) plan).getPredicate());
        } else if (plan instanceof Project) {
            return new Project(transformToLeftDeepTree(((UnaryOperator) plan).getInput()), ((Project) plan).getAttributes());
        }
        // 如果是其他操作符，直接返回
        return plan;
    }

    private Operator pushdownSelections(Operator plan) {
        if (plan instanceof Select) {
            Predicate predicate = ((Select) plan).getPredicate();
            Operator input = ((Select) plan).getInput();

            // 尝试将选择操作推到更下层
            Operator newInput = pushdownSelections(input);

            // 检查新的输入是否可以进一步推下选择
            return handleSelection(predicate, newInput);
        } else if (plan instanceof Project) {
            // 对于单元操作符，递归处理其子节点
            Operator newInput = pushdownSelections(((UnaryOperator) plan).getInput());
            return new Project(newInput, ((Project) plan).getAttributes());
        } else if (plan instanceof BinaryOperator) {
            // 对于二元操作符，独立处理两个子节点
            Operator newLeft = pushdownSelections(((BinaryOperator) plan).getLeft());
            Operator newRight = pushdownSelections(((BinaryOperator) plan).getRight());
            if (plan instanceof Product) {
                return new Product(newLeft, newRight);
            } else if (plan instanceof Join) {
                return new Join(newLeft, newRight, ((Join) plan).getPredicate());
            }
        }
        // 如果是Scan或其他操作符类型，直接返回
        return plan;
    }

    private Operator handleSelection(Predicate predicate, Operator newInput) {
        if (newInput instanceof Scan) {
            // 如果输入是Scan，直接应用选择
            return new Select(newInput, predicate);
        } else if (newInput instanceof Product) {
            // 如果新的输入是Product操作符，根据谓词涉及的属性决定是否可以推下
            if (canBePushedToLeftSubtree(predicate, newInput)) {
                // 如果谓词涉及的属性全部来自左子树
                BinaryOperator binOp = (BinaryOperator) newInput;
                Operator left = pushdownSelections(new Select(binOp.getLeft(), predicate));
                return new Product(left, binOp.getRight());
            } else if (canBePushedToRightSubtree(predicate, newInput)) {
                // 如果谓词涉及的属性全部来自右子树
                BinaryOperator binOp = (BinaryOperator) newInput;
                Operator right = pushdownSelections(new Select(binOp.getRight(), predicate));
                return new Product(binOp.getLeft(), right);
            }
        } else if (newInput instanceof UnaryOperator) {
            // Skip the UnaryOperator and try to push the selection down its input
            Operator inputOfUnary = ((UnaryOperator) newInput).getInput();
            Operator pushed = handleSelection(predicate, inputOfUnary);

            // Reconstruct the UnaryOperator with the pushed selection
            if (newInput instanceof Project) {
                return new Project(pushed, ((Project) newInput).getAttributes());
            } else {
                return new Select(pushed, ((Select) newInput).getPredicate());
            }
        }
        // 如果无法进一步推下选择，保持当前结构
        return new Select(newInput, predicate);
    }


    private boolean canBePushedToLeftSubtree(Predicate predicate, Operator operator) {
        if (!(operator instanceof BinaryOperator)) {
            return false;
        }
        BinaryOperator binOp = (BinaryOperator) operator;
        Operator left = binOp.getLeft();
        return containsAttribute(left, predicate.getLeftAttribute()) &&
                (predicate.getRightAttribute() == null || containsAttribute(left, predicate.getRightAttribute()));
    }

    private boolean canBePushedToRightSubtree(Predicate predicate, Operator operator) {
        if (!(operator instanceof BinaryOperator)) {
            return false;
        }
        BinaryOperator binOp = (BinaryOperator) operator;
        return containsAttribute(binOp.getRight(), predicate.getLeftAttribute()) &&
                (predicate.getRightAttribute() == null || containsAttribute(binOp.getRight(), predicate.getRightAttribute()));
    }

    private boolean containsAttribute(Operator op, Attribute attr) {
        if (op instanceof BinaryOperator) {
            BinaryOperator binOp = (BinaryOperator) op;
            // 递归检查左右子树是否包含该属性
            return containsAttribute(binOp.getLeft(), attr) || containsAttribute(binOp.getRight(), attr);
        }

        if (op instanceof UnaryOperator) {
            // 递归检查子操作符
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
            // Handle the Select operator
            Predicate predicate = ((Select) operator).getPredicate();
            Operator childOperator = ((Select) operator).getInput();

            // If the child is a Product, check if we can form a Join
            if (childOperator instanceof Product) {
                Operator leftChild = ((Product) childOperator).getLeft();
                Operator rightChild = ((Product) childOperator).getRight();

                if (canFormJoin(predicate, leftChild, rightChild)) {
                    // Form a Join with the Select's predicate as the join condition
                    return new Join(combineToJoin(leftChild),
                            combineToJoin(rightChild), predicate);
                }
            }
            // If no join can be formed, apply the logic recursively
            return new Select(combineToJoin(childOperator), predicate);
        } else if (operator instanceof Project) {
            // For unary operators, apply logic to the child
            Operator child = ((UnaryOperator) operator).getInput();
            return new Project(combineToJoin(child), ((Project) operator).getAttributes());
        } else if (operator instanceof Product) {
            // For binary operators, apply logic to both children
            Operator left = combineToJoin(((BinaryOperator) operator).getLeft());
            Operator right = combineToJoin(((BinaryOperator) operator).getRight());
            return new Product(left, right);
        }
        return operator; // If it's a Scan or other type, return it directly
    }

    private boolean canFormJoin(Predicate predicate, Operator leftChild, Operator rightChild) {
        // Check if the predicate references attributes from both the left and right operators
        boolean referencesLeft = referencesAttributes(predicate, leftChild);
        boolean referencesRight = referencesAttributes(predicate, rightChild);
        return referencesLeft && referencesRight;
    }

    private boolean referencesAttributes(Predicate predicate, Operator operator) {
        // Check if any of the attributes in the predicate are produced by the operator
        // This logic will depend on the exact structure of the Predicate and Operator classes
        List<Attribute> attributes = operator.getOutput().getAttributes();
        return attributes.contains(predicate.getLeftAttribute()) ||
                (predicate.getRightAttribute() != null && attributes.contains(predicate.getRightAttribute()));
    }

    private Operator pushdownProjections(Operator plan) {
        if (plan instanceof Project) {
            // 获取当前 Project 节点需要的属性
            Set<Attribute> requiredAttributes = new HashSet<>(((Project) plan).getAttributes());
            // 继续处理投影下面的操作符，同时下推这些属性
            return new Project(pushdownProjectionsRecursive(((Project) plan).getInput(), requiredAttributes), ((Project) plan).getAttributes());
        } else {
            // 如果顶层不是 Project，我们假定所有属性都是需要的
            return pushdownProjectionsRecursive(plan, (Set<Attribute>) plan.getOutput().getAttributes());
        }
    }

    private Operator pushdownProjectionsRecursive(Operator operator, Set<Attribute> attributes) {
        HashSet<Attribute> requiredAttributes = new HashSet<>(attributes);

        if (operator instanceof BinaryOperator) {
            // 对于二元操作符，我们需要确定左右子树分别需要哪些属性
            Set<Attribute> leftAttrs = new HashSet<>();
            Set<Attribute> rightAttrs = new HashSet<>();

            Operator leftOp = ((BinaryOperator) operator).getLeft();
            Operator rightOp = ((BinaryOperator) operator).getRight();

            if (operator instanceof Join) {
                requiredAttributes.add(((Join) operator).getPredicate().getLeftAttribute());
                requiredAttributes.add(((Join) operator).getPredicate().getRightAttribute());
            }


            for (Attribute attr : requiredAttributes) {
                if (operatorContains(leftOp, attr)) {
                    leftAttrs.add(attr);
                }
                if (operatorContains(rightOp, attr)) {
                    rightAttrs.add(attr);
                }
            }

            // 递归处理子树
            Operator left = new Project(pushdownProjectionsRecursive(leftOp, leftAttrs), new ArrayList<>(leftAttrs));
            Operator right = new Project(pushdownProjectionsRecursive(rightOp, rightAttrs), new ArrayList<>(rightAttrs));

            // 重构操作符
            if (operator instanceof Product) {
                return new Product(left, right);
            } else if (operator instanceof Join) {
                return new Join(left, right, ((Join) operator).getPredicate());
            }
        } else if (operator instanceof Project) {
            Operator input = pushdownProjectionsRecursive(((UnaryOperator) operator).getInput(), requiredAttributes);
            return new Project(input, new ArrayList<>(requiredAttributes));
        } else if (operator instanceof Select) {
            Predicate predicate = ((Select) operator).getPredicate();
            requiredAttributes.add(predicate.getLeftAttribute());

            if (predicate.getRightAttribute() != null) {
                requiredAttributes.add(predicate.getRightAttribute());
            }
            Operator input = pushdownProjectionsRecursive(((UnaryOperator) operator).getInput(), requiredAttributes);
            return new Select(input, predicate);
        }

        // 如果没有属性要下推，或者是一个Scan操作符，直接返回
        return operator;
    }

    private boolean operatorContains(Operator operator, Attribute attribute) {
        // 如果 operator 有输出，并且包含给定属性，则返回true
        if (operator.getOutput() != null) {
            for (Attribute attr : operator.getOutput().getAttributes()) {
                if (attr.equals(attribute)) {
                    return true;
                }
            }
        }
        return false;
    }
}
