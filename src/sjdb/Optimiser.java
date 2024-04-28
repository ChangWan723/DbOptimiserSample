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
        plan = pushDownSelections(plan);
        plan = combineToJoin(plan);
        plan = pushDownProjects(plan);
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

    private Operator pushDownSelections(Operator plan) {
        if (plan instanceof Select) {
            Predicate predicate = ((Select) plan).getPredicate();
            Operator input = ((Select) plan).getInput();

            // 尝试将选择操作推到更下层
            Operator newInput = pushDownSelections(input);

            // 检查新的输入是否可以进一步推下选择
            return handleSelection(predicate, newInput);
        } else if (plan instanceof Project) {
            // 对于单元操作符，递归处理其子节点
            Operator newInput = pushDownSelections(((UnaryOperator) plan).getInput());
            return new Project(newInput, ((Project) plan).getAttributes());
        } else if (plan instanceof BinaryOperator) {
            // 对于二元操作符，独立处理两个子节点
            Operator newLeft = pushDownSelections(((BinaryOperator) plan).getLeft());
            Operator newRight = pushDownSelections(((BinaryOperator) plan).getRight());
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
                Operator left = pushDownSelections(new Select(binOp.getLeft(), predicate));
                return new Product(left, binOp.getRight());
            } else if (canBePushedToRightSubtree(predicate, newInput)) {
                // 如果谓词涉及的属性全部来自右子树
                BinaryOperator binOp = (BinaryOperator) newInput;
                Operator right = pushDownSelections(new Select(binOp.getRight(), predicate));
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

            // 如果孩子是一个Product，检查是否可以形成一个 Join
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

    private Operator pushDownProjects(Operator plan) {
        if (plan instanceof Project) {
            // 获取当前 Project 节点需要的属性
            Set<Attribute> requiredAttributes = new HashSet<>(((Project) plan).getAttributes());
            // 继续处理投影下面的操作符，同时下推这些属性
            return new Project(pushDownProjectsRecursive(((Project) plan).getInput(), requiredAttributes), ((Project) plan).getAttributes());
        } else {
            // 如果顶层不是 Project，我们假定所有属性都是需要的
            return pushDownProjectsRecursive(plan, new HashSet<>(getAllAttributes(plan)));
        }
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

    private Operator pushDownProjectsRecursive(Operator operator, Set<Attribute> attributes) {
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
                if (containsAttribute(leftOp, attr)) {
                    leftAttrs.add(attr);
                }
                if (containsAttribute(rightOp, attr)) {
                    rightAttrs.add(attr);
                }
            }

            // 递归处理子树
            Operator left = new Project(pushDownProjectsRecursive(leftOp, leftAttrs), new ArrayList<>(leftAttrs));
            Operator right = new Project(pushDownProjectsRecursive(rightOp, rightAttrs), new ArrayList<>(rightAttrs));

            // 重构操作符
            if (operator instanceof Product) {
                return new Product(left, right);
            } else if (operator instanceof Join) {
                return new Join(left, right, ((Join) operator).getPredicate());
            }
        } else if (operator instanceof Project) {
            Operator input = pushDownProjectsRecursive(((UnaryOperator) operator).getInput(), requiredAttributes);
            return new Project(input, new ArrayList<>(requiredAttributes));
        } else if (operator instanceof Select) {
            Predicate predicate = ((Select) operator).getPredicate();
            requiredAttributes.add(predicate.getLeftAttribute());

            if (predicate.getRightAttribute() != null) {
                requiredAttributes.add(predicate.getRightAttribute());
            }
            Operator input = new Project(pushDownProjectsRecursive(((UnaryOperator) operator).getInput(), requiredAttributes), new ArrayList<>(requiredAttributes));
            return new Select(input, predicate);
        }

        // 如果没有属性要下推，或者是一个Scan操作符，直接返回
        return operator;
    }
}
