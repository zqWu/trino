package io.trino.sql.rewritemv.where;

import io.airlift.log.Logger;
import io.jsonwebtoken.lang.Collections;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.rewritemv.QualifiedColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * 简称 ec
 * colA = colB
 * colA = colC
 * ===>
 * colA = colB = colC
 * 并且这个逻辑在后续的条件中继续起作用
 */
public class EquivalentClass {
    private static final Logger LOG = Logger.get(EquivalentClass.class);
    private final Set<QualifiedColumn> columns;

    public EquivalentClass() {
        columns = new HashSet<>();
    }

    public EquivalentClass(QualifiedColumn c1) {
        requireNonNull(c1);
        columns = new HashSet<>();
        columns.add(c1);
    }

    public EquivalentClass(QualifiedColumn c1, QualifiedColumn c2) {
        requireNonNull(c1);
        requireNonNull(c2);
        columns = new HashSet<>();
        columns.add(c1);
        columns.add(c2);
    }

    public void add(QualifiedColumn c1, QualifiedColumn... c2) {
        requireNonNull(c1);
        columns.add(c1);
        columns.addAll(Arrays.asList(c2));
    }

    public void add(Set<QualifiedColumn> columns) {
        this.columns.addAll(columns);
    }

    public EquivalentClass merge(EquivalentClass o) {
        Set<QualifiedColumn> s1 = new HashSet<>();
        s1.addAll(this.columns);
        s1.addAll(o.columns);
        EquivalentClass ec = new EquivalentClass();
        ec.add(s1);
        return ec;
    }

    public boolean contain(QualifiedColumn q) {
        return columns.contains(q);
    }

    /* 2个ec 是否有交集 */
    public boolean hasCommonColumn(EquivalentClass o) {
        for (QualifiedColumn qsc : columns) {
            if (o.columns.contains(qsc)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (QualifiedColumn q : columns) {
            sb.append(q.getColumnName()).append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public Set<QualifiedColumn> getColumns() {
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
                if (head.hasCommonColumn(ec)) {
                    head = head.merge(ec);
                    mergeCount++;
                } else {
                    notMerged.add(ec);
                }
            }
            notMerged.add(head);
            in = notMerged;
            notMerged = new ArrayList<>();
            LOG.debug(String.format("round %s, in.size()=%s", round, in.size()));
        }

        return in;
    }

    public static void main(String[] args) {
        QualifiedObjectName table = new QualifiedObjectName("c", "s", "db");
        QualifiedColumn colA = new QualifiedColumn(table, "colA");
        QualifiedColumn colB = new QualifiedColumn(table, "colB");
        QualifiedColumn colC = new QualifiedColumn(table, "colC");
        QualifiedColumn colD = new QualifiedColumn(table, "colD");

        QualifiedColumn colE = new QualifiedColumn(table, "colE");
        QualifiedColumn colF = new QualifiedColumn(table, "colF");
        QualifiedColumn colG = new QualifiedColumn(table, "colG");
        QualifiedColumn colH = new QualifiedColumn(table, "colH");

        EquivalentClass e1 = new EquivalentClass(colA, colB); // A=B
        EquivalentClass e2 = new EquivalentClass(colC, colD); // C=D
        EquivalentClass e3 = new EquivalentClass(colA, colC); // A=C

        EquivalentClass e4 = new EquivalentClass(colE, colF); // E=F
        EquivalentClass e5 = new EquivalentClass(colG, colH); // G=H
        EquivalentClass e6 = new EquivalentClass(colE, colG); // E=G

        List<EquivalentClass> list = Arrays.asList(e1, e2, e3, e4, e5, e6);
        List<EquivalentClass> last = fullMerge(list);
        System.out.println(last); // [colH,colE,colF,colG, colD,colA,colB,colC]
    }

}
