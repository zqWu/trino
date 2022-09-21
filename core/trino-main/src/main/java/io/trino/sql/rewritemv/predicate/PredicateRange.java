package io.trino.sql.rewritemv.predicate;

import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;

import java.math.BigDecimal;
import java.util.Objects;

import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;

/**
 * expression: colA >,>=,=,<=,< constant
 * eg. colA > 3, colA <= 7
 */
public class PredicateRange extends Predicate {

    public static class PredicateRangeBound {
        private final ComparisonExpression.Operator op;
        private final Literal value;

        public PredicateRangeBound(ComparisonExpression.Operator op, Literal value) {
            this.op = op;
            this.value = value;
        }

        public static final PredicateRangeBound UNBOUND = new PredicateRangeBound(EQUAL, null);

        public static PredicateRangeBound mergeLower(PredicateRangeBound a, PredicateRangeBound b) {
            if (a == UNBOUND || b == UNBOUND) {
                return (a == UNBOUND) ? b : a;
            }

            // merge lower时, value 取大 . value相同时, 取 > 而不是 >=
            // a > 5 and a >= 10 ===> a>=10
            // a > 3 and a >=3   ===> a>3
            int comp = PredicateRange.compareLiteralValue(a.getValue(), b.getValue());
            if (comp < 0) {
                return b;
            } else if (comp > 0) {
                return a;
            } else {
                if (a.getOp() == GREATER_THAN) {
                    return a;
                } else {
                    return b;
                }
            }
        }

        public static PredicateRangeBound mergeUpper(PredicateRangeBound a, PredicateRangeBound b) {
            if (a == UNBOUND || b == UNBOUND) {
                return (a == UNBOUND) ? b : a;
            }

            // merge lower时, value 取小 . value相同时, 取 < 而不是 <=
            // a < 5 and a <= 10 ===> a<5
            // a < 3 and a <=3   ===> a>3
            int comp = PredicateRange.compareLiteralValue(a.getValue(), b.getValue());
            if (comp < 0) {
                return a;
            } else if (comp > 0) {
                return b;
            } else {
                if (a.getOp() == ComparisonExpression.Operator.LESS_THAN) {
                    return a;
                } else {
                    return b;
                }
            }
        }

        public static PredicateRangeBound mergeEqual(PredicateRangeBound a, PredicateRangeBound b) {
            if (a == UNBOUND || b == UNBOUND) {
                return (a == UNBOUND) ? b : a;
            }

            // 2个都不相同的情况下, 因为限定了是 Literal, 只能相等, 否则就冲突 a=1 and a=2
            int comp = PredicateRange.compareLiteralValue(a.getValue(), b.getValue());
            if (comp == 0) {
                return a;
            }
            throw new RuntimeException("2个 字面量 无法相等");
        }

        public boolean coverOther(PredicateRangeBound o) {
            if (this == UNBOUND) {
                return true;
            }
            if (o == UNBOUND) {
                return false;
            }

            // =
            if (op == EQUAL) {
                if (o.op != EQUAL) {
                    return false;
                } else {
                    int comp = compareLiteralValue(value, o.value);
                    return comp == 0;
                }
            }

            // >, >=
            else if (op == GREATER_THAN || op == GREATER_THAN_OR_EQUAL) {
                if (o.op == LESS_THAN || o.op == LESS_THAN_OR_EQUAL) {
                    return false;
                }

                // o.op = [ >, >=, = ]
                int comp = compareLiteralValue(value, o.value);
                if (comp < 0) {
                    // a>1 | a>=1 cover  a>2
                    // a>1 | a>=1 cover  a>=2
                    // a>1 | a>=1 cover  a=2
                    return true;
                }
                if (comp > 0) {
                    return false;
                }
                // comp == 0
                if (o.op == op) {
                    // a>1 cover a>1
                    // a>=1 cover a>=1
                    return true;
                }
                if (op == GREATER_THAN_OR_EQUAL && (o.op == EQUAL || o.op == GREATER_THAN)) {
                    // a>=1 covers a=1
                    return true;
                }
                return false;
            }

            // <, <=
            if (op == LESS_THAN || op == LESS_THAN_OR_EQUAL) {
                if (o.op == GREATER_THAN || o.op == GREATER_THAN_OR_EQUAL) {
                    return false;
                }
                int comp = compareLiteralValue(value, o.value);
                if (comp > 0) {
                    // a<10 | a<=10 cover  a<5
                    // a<10 | a<=10 cover  a<=5
                    // a<10 | a<=10 cover  a=5
                    return true;
                }
                if (comp < 0) {
                    return false;
                }
                // comp == 0
                if (o.op == op) {
                    return true;
                }
                if (op == LESS_THAN_OR_EQUAL && (o.op == EQUAL || o.op == LESS_THAN)) {
                    return true;
                }
                return false;
            }

            // should not be here
            return false;
        }

        @Override
        public String toString() {
            if (this == UNBOUND) {
                return "UNBOUND";
            }
            return op.getValue() + value.toString();
        }

        // ======== get
        public ComparisonExpression.Operator getOp() {
            return op;
        }

        public Literal getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PredicateRangeBound)) return false;
            PredicateRangeBound that = (PredicateRangeBound) o;
            return op == that.op && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(op, value);
        }
    }

    private final QualifiedColumn left;
    private EquivalentClass ec;
    private PredicateRangeBound lower;
    private PredicateRangeBound equal;
    private PredicateRangeBound upper;

    public PredicateRange(Expression expr, QualifiedColumn left, Literal right, ComparisonExpression.Operator op) {
        super(expr, PredicateType.COLUMN_RANGE);
        if (!validOperator(op)) {
            throw new RuntimeException("not support operator:" + op);
        }
        if (!validLiteral(right)) {
            throw new RuntimeException("not support Literal:" + right);
        }

        this.left = left;
        if (EQUAL == op) {
            this.equal = new PredicateRangeBound(EQUAL, right);
            this.lower = PredicateRangeBound.UNBOUND;
            this.upper = PredicateRangeBound.UNBOUND;
        } else if (GREATER_THAN == op
                || GREATER_THAN_OR_EQUAL == op) {
            this.lower = new PredicateRangeBound(op, right);
            this.equal = PredicateRangeBound.UNBOUND;
            this.upper = PredicateRangeBound.UNBOUND;
        } else if (ComparisonExpression.Operator.LESS_THAN == op
                || ComparisonExpression.Operator.LESS_THAN_OR_EQUAL == op) {
            this.upper = new PredicateRangeBound(op, right);
            this.equal = PredicateRangeBound.UNBOUND;
            this.lower = PredicateRangeBound.UNBOUND;
        }
    }

    public static PredicateRange fromRange(Expression expr, QualifiedColumn left, Literal min, Literal max) {
        PredicateRange range = new PredicateRange(expr, left, min, GREATER_THAN_OR_EQUAL);
        range.upper = new PredicateRangeBound(LESS_THAN_OR_EQUAL, max);
        return range;
    }

    private PredicateRange(QualifiedColumn left, EquivalentClass ec) {
        super(null, PredicateType.COLUMN_RANGE);
        this.left = left;
        this.ec = ec;
    }

    public static PredicateRange unbound(QualifiedColumn left, EquivalentClass ec) {
        PredicateRange pr = new PredicateRange(left, ec);
        pr.lower = PredicateRangeBound.UNBOUND;
        pr.equal = PredicateRangeBound.UNBOUND;
        pr.upper = PredicateRangeBound.UNBOUND;
        return pr;
    }

    /**
     * range的交集
     */
    public PredicateRange intersection(PredicateRange o) {
        PredicateRange pr = new PredicateRange(this.left, this.ec);
        PredicateRangeBound mergedEqual = PredicateRangeBound.mergeLower(this.equal, o.equal);
        PredicateRangeBound mergedLower = PredicateRangeBound.mergeLower(this.lower, o.lower);
        PredicateRangeBound mergedUpper = PredicateRangeBound.mergeLower(this.upper, o.upper);

        pr.lower = mergedLower;
        pr.equal = mergedEqual;
        pr.upper = mergedUpper;

        return pr;
    }

    public static boolean validOperator(ComparisonExpression.Operator op) {
        return EQUAL == op
                || GREATER_THAN == op
                || GREATER_THAN_OR_EQUAL == op
                || ComparisonExpression.Operator.LESS_THAN == op
                || ComparisonExpression.Operator.LESS_THAN_OR_EQUAL == op;
    }

    public static boolean validLiteral(Literal l) {
        return l instanceof DecimalLiteral
                || l instanceof DoubleLiteral
                || l instanceof IntervalLiteral
                || l instanceof LongLiteral
                || l instanceof StringLiteral
                || l instanceof TimeLiteral
                || l instanceof TimestampLiteral;
    }

    // 参考 LiteralInterpreter.java
    private static int compareLiteralValue(Literal l1, Literal l2) {
        Class<? extends Literal> clazz1 = l1.getClass();
        Class<? extends Literal> clazz2 = l2.getClass();
        if (clazz1 != clazz2) {
            throw new RuntimeException("must be same class");
        }

        if (l1 instanceof DecimalLiteral) {
            BigDecimal v1 = new BigDecimal(((DecimalLiteral) l1).getValue());
            BigDecimal v2 = new BigDecimal(((DecimalLiteral) l2).getValue());
            return v1.compareTo(v2);
        }

        if (l1 instanceof DoubleLiteral) {
            Double v1 = ((DoubleLiteral) l1).getValue();
            Double v2 = ((DoubleLiteral) l2).getValue();
            return v1.compareTo(v2);
        }

        if (l1 instanceof IntervalLiteral) {
            Long v1 = visitIntervalLiteral((IntervalLiteral) l1);
            Long v2 = visitIntervalLiteral((IntervalLiteral) l2);
            return v1.compareTo(v2);
        }

        if (l1 instanceof LongLiteral) {
            Long v1 = ((LongLiteral) l1).getValue();
            Long v2 = ((LongLiteral) l2).getValue();
            return v1.compareTo(v2);
        }

        if (l1 instanceof StringLiteral) {
            String v1 = ((StringLiteral) l1).getValue();
            String v2 = ((StringLiteral) l2).getValue();
            return v1.compareTo(v2);
        }

        if (l1 instanceof TimeLiteral) {
            Long v1 = parseTime(((TimeLiteral) l1).getValue());
            Long v2 = parseTime(((TimeLiteral) l2).getValue());
            return v1.compareTo(v2);
        }

        if (l1 instanceof TimestampLiteral) {
            Long v1 = (Long) parseTimestamp(MAX_SHORT_PRECISION, ((TimestampLiteral) l1).getValue());
            Long v2 = (Long) parseTimestamp(MAX_SHORT_PRECISION, ((TimestampLiteral) l2).getValue());
            return v1.compareTo(v2);
        }

        throw new RuntimeException("TODO: StringLiteral TimeLiteral TimestampLiteral");
    }

    private static long visitIntervalLiteral(IntervalLiteral node) {
        if (node.isYearToMonth()) {
            return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
        }
        return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
    }


    public boolean coverOther(PredicateRange o) {
        if (!Objects.equals(left, o.left)) {
            return false;
        }
        return coverOtherValue(o);
    }

    public boolean coverOtherValue(PredicateRange o) {
        // lower cover check
        boolean cover = lower.coverOther(o.lower);
        if (cover) {
            cover = equal.coverOther(o.equal);
        }
        if (cover) {
            cover = upper.coverOther(o.upper);
        }
        return cover;
    }

    // large比self的范围大
    public PredicateRange baseOn(PredicateRange large) {
        PredicateRange onTop = unbound(left, ec);

        // equal
        if (equal != PredicateRangeBound.UNBOUND) { // 只需要返回 equal中的条件 即可
            if (large.getEqual() == PredicateRangeBound.UNBOUND) {
                onTop.equal = this.equal;
            }
        } else {
            // lower
            if (this.lower != PredicateRangeBound.UNBOUND) {
                if (!Objects.equals(this.lower, large.lower)) {
                    onTop.lower = this.lower;
                }
            }

            // upper
            if (this.upper != PredicateRangeBound.UNBOUND) {
                if (!Objects.equals(this.upper, large.upper)) {
                    onTop.upper = this.upper;
                }
            }
        }
        return onTop;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(left).append(" [")
                .append(" ").append(lower)
                .append(", ").append(equal)
                .append(", ").append(upper)
                .append(" ]");
        return sb.toString();
    }

    // ======== get

    public EquivalentClass getEc() {
        return ec;
    }

    public void setEc(EquivalentClass ec) {
        this.ec = ec;
    }

    public QualifiedColumn getLeft() {
        return left;
    }

    public PredicateRangeBound getLower() {
        return lower;
    }

    public PredicateRangeBound getEqual() {
        return equal;
    }

    public PredicateRangeBound getUpper() {
        return upper;
    }

    // ========= main
    public static void main(String[] args) {
        // cover
        QualifiedObjectName table = new QualifiedObjectName("c", "s", "db");
        QualifiedColumn colA = new QualifiedColumn(table, "colA");
        LongLiteral l1 = new LongLiteral("5");
        LongLiteral l2 = new LongLiteral("3");
        LongLiteral l3 = new LongLiteral("8");

        PredicateRange pr1 = new PredicateRange(null, colA, l1, EQUAL);                 // colA = 5
        PredicateRange pr2 = new PredicateRange(null, colA, l2, GREATER_THAN);          // colA > 3
        PredicateRange pr5 = new PredicateRange(null, colA, l2, GREATER_THAN_OR_EQUAL); // colA >= 3
        PredicateRange pr3 = new PredicateRange(null, colA, l3, LESS_THAN_OR_EQUAL);    // colA <=8
        PredicateRange pr4 = new PredicateRange(null, colA, l3, LESS_THAN);             // colA <8
        PredicateRange pr1pr2 = pr1.intersection(pr2);                                     // >3, =5,
        PredicateRange pr2pr3 = pr3.intersection(pr2);                                     // >3, <=8

        System.out.println("pr2.coverOther(pr1) = " + pr2.coverOther(pr1));
        System.out.println("pr3.coverOther(pr1) = " + pr3.coverOther(pr1));
        System.out.println("pr3.coverOther(pr4) = " + pr3.coverOther(pr4));
        System.out.println("pr5.coverOther(pr2) = " + pr5.coverOther(pr2));

        System.out.println("pr1pr2= " + pr1pr2);
        PredicateRange pr1x = pr1pr2.baseOn(pr2);
        System.out.println("pr1x= " + pr1x);


        System.out.println("pr2pr3= " + pr2pr3);
        PredicateRange pr3x = pr2pr3.baseOn(pr2);
        System.out.println("pr3x= " + pr3x);

    }
}
