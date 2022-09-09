package io.trino.sql.rewritemv.rewriter;

import io.jsonwebtoken.lang.Collections;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.tree.Expression;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * 逻辑等价:
 * where colA = colB and colA = 1
 * where colA = colB and colB = 1
 * 此类为了实现这一逻辑
 */
public class EquivalentClass {
    private Expression value;
    private Set<QualifiedSingleColumn> columns;

    public EquivalentClass(QualifiedSingleColumn c1, QualifiedSingleColumn c2) {
        requireNonNull(c1);
        requireNonNull(c2);
        columns = new HashSet<>();
        columns.add(c1);
        columns.add(c2);
    }

    public EquivalentClass(EqualWhere equalWhere) {
        this(equalWhere.getColLeft(), equalWhere.getColRight());
    }

    public EquivalentClass(Expression value, Set<QualifiedSingleColumn> columns) {
        this.value = value;
        this.columns = columns;
    }

    public EquivalentClass merge(EquivalentClass o) {
        Set<QualifiedSingleColumn> s1 = new HashSet<>();
        s1.addAll(columns);
        s1.addAll(o.columns);
        return new EquivalentClass(value, s1);
    }

    public boolean contain(QualifiedSingleColumn q) {
        return columns.contains(q);
    }

    public boolean equivalent(EquivalentClass o) {
        for (QualifiedSingleColumn qsc : columns) {
            if (o.columns.contains(qsc)) {
                return true;
            }
        }
        return false;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (QualifiedSingleColumn q : columns) {
            sb.append(q.getColumnName()).append(',');
        }
        return sb.toString();
    }

    public boolean setValueIfNecessary(Expression value) {
        if (this.value == null) {
            this.value = value;
            return true;
        } else {
            return Objects.equals(this.value, value);
        }
    }

    public Expression getValue() {
        return value;
    }

    public Set<QualifiedSingleColumn> getColumns() {
        return columns;
    }

    public static List<EquivalentClass> fullMerge(List<EquivalentClass> in) {
        if (Collections.isEmpty(in) || in.size() < 1) {
            return in;
        }

        int mergeCount = Integer.MAX_VALUE;
        List<EquivalentClass> notMerged = new ArrayList<>();
        int round = 0;
        while (mergeCount > 0) {
            mergeCount = 0;
            round++;
            EquivalentClass head = in.get(0);
            for (int i = 1; i < in.size(); i++) {
                EquivalentClass ec = in.get(i);
                if (head.equivalent(ec)) {
                    head = head.merge(ec);
                    mergeCount++;
                } else {
                    notMerged.add(ec);
                }
            }
            notMerged.add(head);
            in = notMerged;
            notMerged = new ArrayList<>();
            System.out.println(String.format("round %s, in.size()=%s", round, in.size()));
        }

        return in;
    }

    public static void main(String[] args) {
        QualifiedObjectName table = new QualifiedObjectName("c", "s", "db");
        QualifiedSingleColumn colA = new QualifiedSingleColumn(table, "colA");
        QualifiedSingleColumn colB = new QualifiedSingleColumn(table, "colB");
        QualifiedSingleColumn colC = new QualifiedSingleColumn(table, "colC");
        QualifiedSingleColumn colD = new QualifiedSingleColumn(table, "colD");

        QualifiedSingleColumn colE = new QualifiedSingleColumn(table, "colE");
        QualifiedSingleColumn colF = new QualifiedSingleColumn(table, "colF");
        QualifiedSingleColumn colG = new QualifiedSingleColumn(table, "colG");
        QualifiedSingleColumn colH = new QualifiedSingleColumn(table, "colH");

        EquivalentClass e1 = new EquivalentClass(colA, colB); // A=B
        EquivalentClass e2 = new EquivalentClass(colC, colD); // C=D
        EquivalentClass e3 = new EquivalentClass(colA, colC); // A=C

        EquivalentClass e4 = new EquivalentClass(colE, colF); // E=F
        EquivalentClass e5 = new EquivalentClass(colG, colH); // G=H
        EquivalentClass e6 = new EquivalentClass(colE, colG); // E=G

        List<EquivalentClass> list = Arrays.asList(e1, e2, e3, e4, e5, e6);
        List<EquivalentClass> last = fullMerge(list);
        System.out.println(last);
    }

}
