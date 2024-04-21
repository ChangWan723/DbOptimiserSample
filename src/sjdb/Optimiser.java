package sjdb;

import java.util.ArrayList;
import java.util.List;

public class Optimiser {
    private Catalogue catalogue;

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    public Operator optimise(Operator plan) {
        plan = transformToLeftDeepTree(plan);
        plan = pushdownSelections(plan);
        /*plan = optimizeJoinOrder(plan);
        plan = combineProductsToCreateJoins(plan);
        plan = pushdownProjections(plan);*/
        return plan;
    }

    private Operator optimizeJoinOrder(Operator plan) {
        // 假设输入是一系列的Product操作符
        if (!(plan instanceof Product)) {
            return plan;
        }

        // 实际优化逻辑将更复杂，这里简化为总是将左子树深度最小的操作符与右侧进行连接
        List<Operator> operands = new ArrayList<>();
        while (plan instanceof Product) {
            operands.add(((Product) plan).getRight());
            plan = ((Product) plan).getLeft();
        }
        operands.add(plan);

        // 重新组织操作符，以优化连接顺序
        Operator result = operands.remove(0);
        for (Operator operand : operands) {
            result = new Product(result, operand);
        }
        return result;
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
        } else if (newInput instanceof BinaryOperator) {
            // 如果新的输入是二元操作符，根据谓词涉及的属性决定是否可以推下
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
        }
        // 如果无法进一步推下选择，保持当前结构
        return new Select(newInput, predicate);
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


    private Operator combineProductsToCreateJoins(Operator plan) {
        // 查找可以转换成Join的Product和Select组合
        if (plan instanceof Product) {
            Product product = (Product) plan;
            Operator left = combineProductsToCreateJoins(product.getLeft());
            Operator right = combineProductsToCreateJoins(product.getRight());

            // 查看右侧是否有Select操作符，这可能指示可以进行Join
            if (right instanceof Select) {
                Select select = (Select) right;
                // 检查谓词是否为Equi-Join条件
                if (select.getPredicate().getRightAttribute() != null && select.getPredicate().getRightValue() == null) {
                    // 如果是基于属性的等值连接，创建Join操作符
                    return new Join(left, select.getInput(), select.getPredicate());
                }
            }
            // 如果不满足转换条件，返回原始的Product操作符
            return new Product(left, right);
        } else if (plan instanceof UnaryOperator) {
            // 对单一操作符递归处理其输入
            Operator input = ((UnaryOperator) plan).getInput();
            Operator newInput = combineProductsToCreateJoins(input);
            if (plan instanceof Select) {
                return new Select(newInput, ((Select) plan).getPredicate());
            } else if (plan instanceof Project) {
                return new Project(newInput, ((Project) plan).getAttributes());
            }
        }
        // 对于Scan和其他类型的操作符，直接返回
        return plan;
    }

    private Operator pushdownProjections(Operator plan) {
        // 查找需要的属性，如果最顶层是Project操作符，提取所需属性
        if (plan instanceof Project) {
            List<Attribute> requiredAttributes = new ArrayList<>(((Project) plan).getAttributes());
            // 移除最顶层的Project，因为我们将尝试将投影尽可能向下推
            plan = ((Project) plan).getInput();
            // 递归地推下投影，并创建新的操作符实例，而非修改现有实例
            return pushdownProjectionsRecursive(plan, requiredAttributes);
        } else {
            // 如果最顶层不是Project，直接递归处理
            return pushdownProjectionsRecursive(plan, null);
        }
    }

    private Operator pushdownProjectionsRecursive(Operator plan, List<Attribute> requiredAttributes) {
        if (plan instanceof Scan) {
            // 如果是Scan操作符，直接在此处应用投影
            if (requiredAttributes != null) {
                return new Project(plan, requiredAttributes);
            }
            return plan;
        } else if (plan instanceof UnaryOperator) {
            // 对于UnaryOperator，递归处理其子节点
            Operator newInput = pushdownProjectionsRecursive(((UnaryOperator) plan).getInput(), requiredAttributes);
            // 根据操作符类型重新创建UnaryOperator
            if (plan instanceof Select) {
                return new Select(newInput, ((Select) plan).getPredicate());
            } else if (plan instanceof Project) {
                // 更新Project操作符的属性
                return new Project(newInput, requiredAttributes != null ? requiredAttributes : ((Project) plan).getAttributes());
            }
        } else if (plan instanceof BinaryOperator) {
            // 对于BinaryOperator，递归处理两个子节点
            Operator newLeft = pushdownProjectionsRecursive(((BinaryOperator) plan).getLeft(), requiredAttributes);
            Operator newRight = pushdownProjectionsRecursive(((BinaryOperator) plan).getRight(), requiredAttributes);
            // 根据操作符类型重新创建BinaryOperator
            if (plan instanceof Product) {
                return new Product(newLeft, newRight);
            } else if (plan instanceof Join) {
                return new Join(newLeft, newRight, ((Join) plan).getPredicate());
            }
        }
        // 对于其他类型的操作符，直接返回
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
}
