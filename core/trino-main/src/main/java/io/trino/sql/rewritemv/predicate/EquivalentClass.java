package io.trino.sql.rewritemv.predicate;

import io.airlift.log.Logger;
import io.jsonwebtoken.lang.Collections;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.rewritemv.QualifiedColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import static java.util.Objects.requireNonNull;

/**
 * 简称 ec
 * colA = colB
 * colA = colC
 * ===>
 * colA = colB = colC
 * 并且这个逻辑在后续的条件中继续起作用
 */
public class EquivalentClass
{
    private static final Logger LOG = Logger.get(EquivalentClass.class);
    private final Set<QualifiedColumn> columns;

    public EquivalentClass()
    {
        columns = new HashSet<>();
    }

    public EquivalentClass(QualifiedColumn c1)
    {
        requireNonNull(c1);
        columns = new HashSet<>();
        columns.add(c1);
    }

    public EquivalentClass(QualifiedColumn c1, QualifiedColumn c2)
    {
        requireNonNull(c1);
        requireNonNull(c2);
        columns = new HashSet<>();
        columns.add(c1);
        columns.add(c2);
    }

    public void add(QualifiedColumn c1, QualifiedColumn... c2)
    {
        requireNonNull(c1);
        columns.add(c1);
        columns.addAll(Arrays.asList(c2));
    }

    public void add(Set<QualifiedColumn> columns)
    {
        this.columns.addAll(columns);
    }

    public EquivalentClass merge(EquivalentClass o)
    {
        Set<QualifiedColumn> s1 = new HashSet<>();
        s1.addAll(this.columns);
        s1.addAll(o.columns);
        EquivalentClass ec = new EquivalentClass();
        ec.add(s1);
        return ec;
    }

    public EquivalentClass merge(List<EquivalentClass> l)
    {
        Set<QualifiedColumn> s1 = new HashSet<>();
        s1.addAll(this.columns);
        for (EquivalentClass ec : l) {
            s1.addAll(ec.columns);
        }
        EquivalentClass ec = new EquivalentClass();
        ec.add(s1);
        return ec;
    }

    public boolean contain(QualifiedColumn q)
    {
        return columns.contains(q);
    }

    /* 2个ec 是否有交集 */
    public boolean hasCommonColumn(EquivalentClass o)
    {
        for (QualifiedColumn qsc : columns) {
            if (o.columns.contains(qsc)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (QualifiedColumn q : columns) {
            sb.append(q.getColumnName()).append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append('}');
        return sb.toString();
    }

    public Set<QualifiedColumn> getColumns()
    {
        return columns;
    }

    /**
     * input = [ (a,b), (c,d), (e,f), (a,c), (d,e), (m,n)]
     * 步骤如下:
     * 1. 取input[0] 作为 head, 这里 head=(a,b)
     * 2. 遍历剩下的 input
     * 2.1 如果发现一个可以与head进行合并的元素, 如(a,c), 则, 更新 head=(a,b,c), 删除input中的(a,c)
     * 重新从1执行, head=(a,b,c)
     * 2.2 如果 遍历整个 input 都没有与 head可合并的元素
     * 保存当前head, 另取 head=input[0]
     * 3. 当 input为空时, 合并结束
     * eg.
     * input0 = [ (a,b), (c,d), (e,f), (a,c), (d,e), (m,n), (p,q) ]
     * head = (a,b), input = [(c,d), (e,f), (a,c), (d,e), (m,n), (p,q)], save=[]
     * 遍历, (a,c) 与 head 交集a, 可合并
     * head = (a,b,c), input = [(c,d), (e,f), (d,e), (m,n), (p,q)], save=[]
     * 遍历, (c,d)与 head 交集c, 可合并
     * head = (a,b,c,d), input = [(e,f), (d,e), (m,n), (p,q)], save=[]
     * head = (a,b,c,d,e), input = [(e,f), (m,n)], save=[]
     * head = (a,b,c,d,e,f), input = [(m,n), (p,q)], save=[]
     * head = (m,n), input = [ (p,q)], save=[(a,b,c,d,e,f)]
     * head = (p,q) input = [ ], save=[(a,b,c,d,e,f), (m,n)]
     * head = input = [ ], save=[(a,b,c,d,e,f), (m,n), (p,q) ]
     * 结束, save就是返回的结果
     *
     * @param in
     * @return
     */
    public static List<EquivalentClass> fullMerge(List<EquivalentClass> in)
    {
        if (Collections.isEmpty(in) || in.size() <= 1) {
            return in;
        }

        List<EquivalentClass> save = new ArrayList<>(in.size());
        Vector<EquivalentClass> vector = new Vector<>(in);
        EquivalentClass head = vector.remove(0);

        while (vector.size() > 0) {
            List<EquivalentClass> canMergeHead = new ArrayList<>();
            for (EquivalentClass ec : vector) {
                if (head.hasCommonColumn(ec)) {
                    canMergeHead.add(ec);
                }
            }

            if (canMergeHead.size() > 0) {
                vector.removeAll(canMergeHead);
                head = head.merge(canMergeHead);
            }
            else {
                save.add(head);
                head = vector.remove(0);
            }

            if (vector.size() == 0) {
                save.add(head);
            }
        }

        return save;
    }

    public static void main(String[] args)
    {
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

        List<EquivalentClass> list;
        List<EquivalentClass> merged;

        list = Arrays.asList(e1);
        merged = fullMerge(list);
        System.out.println(merged); // [{colA,colB}]

        list = Arrays.asList(e1, e2);
        merged = fullMerge(list);
        System.out.println(merged); // [{colA,colB}, {colD,colC}]

        list = Arrays.asList(e1, e1);
        merged = fullMerge(list);
        System.out.println(merged); // [{colA,colB}]

        list = Arrays.asList(e1, e2, e2);
        merged = fullMerge(list);
        System.out.println(merged); // [{colA,colB}, {colD,colC}]

        list = Arrays.asList(e1, e2, e3);
        merged = fullMerge(list);
        System.out.println(merged); // [{colD,colA,colB,colC}]

        list = Arrays.asList(e1, e2, e3, e1, e2, e3);
        merged = fullMerge(list);
        System.out.println(merged); // [{colD,colA,colB,colC}]

        list = Arrays.asList(e1, e2, e3, e4, e5, e6);
        merged = fullMerge(list);
        System.out.println(merged); // [{colD,colA,colB,colC}, {colH,colE,colF,colG}]
    }
}
