package io.trino.sql.rewritemv.where;

import io.trino.sql.rewritemv.QualifiedColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class WhereAnalysis {
    private boolean support = true;     // false when not support
    private String reason;              // why not support
    private final List<PredictEqual> peList = new ArrayList<>();
    private final List<PredictRange> prList = new ArrayList<>();
    private final List<PredictOther> puList = new ArrayList<>();
    private List<EquivalentClass> ecList;

    // 不要对 EMPTY_WHERE 进行任何写操作
    public static final WhereAnalysis EMPTY_WHERE = new WhereAnalysis();

    public WhereAnalysis() {
    }

    public void notSupport(String reason) {
        support = false;
        this.reason = reason;
    }

    public void build() {
        // 整理 equivalent class
        List<EquivalentClass> ecb = new ArrayList<>(peList.size());
        for (PredictEqual pe : peList) {
            if (!pe.isAlwaysTrue()) {
                ecb.add(new EquivalentClass(pe.getLeft(), pe.getRight()));
            }
        }
        for (PredictRange pr : prList) {
            ecb.add(new EquivalentClass(pr.getLeft()));
        }
        ecList = EquivalentClass.fullMerge(ecb);

        // 处理 range: 每个range加上关联的
        for (PredictRange pr : prList) {
            QualifiedColumn col = pr.getLeft();
            EquivalentClass targetEc = ecList.stream().filter(ec -> ec.contain(col)).findAny().get();
            pr.setEc(targetEc);
        }

    }

    public void addPredict(AtomicWhere p) {
        if (p instanceof PredictEqual) {
            peList.add((PredictEqual) p);
        } else if (p instanceof PredictRange) {
            prList.add((PredictRange) p);
        }

        // others put in predictOther
        else {
            puList.add((PredictOther) p);
        }
    }

    /**
     * 是否包含有效的 where 条件 == 有些条件不是always true
     */
    public boolean hasEffectivePredict() {
        if (this == EMPTY_WHERE) {
            return false;
        }

        List<AtomicWhere> allPredict = new ArrayList<>();
        allPredict.addAll(peList);
        allPredict.addAll(prList);
        allPredict.addAll(puList);

        if (allPredict.size() == 0) {
            return false;
        }

        Optional<?> any = allPredict.stream().filter(AtomicWhere::isAlwaysTrue).findAny();
        return any.isPresent();
    }

    // ==================== getter ==========

    public List<EquivalentClass> getEcList() {
        return ecList;
    }

    public List<PredictEqual> getPeList() {
        return peList;
    }

    public List<PredictRange> getPrList() {
        return prList;
    }

    public List<PredictOther> getPuList() {
        return puList;
    }

    public String getReason() {
        return reason;
    }

    public boolean isSupport() {
        return support;
    }
}
