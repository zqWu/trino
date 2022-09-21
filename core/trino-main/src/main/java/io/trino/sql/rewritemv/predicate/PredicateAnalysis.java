package io.trino.sql.rewritemv.predicate;

import io.trino.sql.rewritemv.QualifiedColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PredicateAnalysis {
    private boolean support = true;     // false when not support
    private String reason;              // why not support
    private final List<PredicateEqual> equalList = new ArrayList<>();
    private final List<PredicateRange> rangeList = new ArrayList<>();
    private final List<PredicateOther> otherList = new ArrayList<>();
    private List<EquivalentClass> ecList;

    // 不要对 EMPTY_PREDICATE 进行任何写操作
    public static final PredicateAnalysis EMPTY_PREDICATE = new PredicateAnalysis();

    public PredicateAnalysis() {
    }

    public void notSupport(String reason) {
        support = false;
        this.reason = reason;
    }

    public void build() {
        // 整理 equivalent class
        List<EquivalentClass> ecb = new ArrayList<>(equalList.size());
        for (PredicateEqual pe : equalList) {
            if (!pe.isAlwaysTrue()) {
                ecb.add(new EquivalentClass(pe.getLeft(), pe.getRight()));
            }
        }
        for (PredicateRange pr : rangeList) {
            ecb.add(new EquivalentClass(pr.getLeft()));
        }
        ecList = EquivalentClass.fullMerge(ecb);

        // 处理 range: 每个range加上关联的
        for (PredicateRange pr : rangeList) {
            QualifiedColumn col = pr.getLeft();
            EquivalentClass targetEc = ecList.stream().filter(ec -> ec.contain(col)).findAny().get();
            pr.setEc(targetEc);
        }

    }

    public void addPredicate(Predicate p) {
        if (p instanceof PredicateEqual) {
            equalList.add((PredicateEqual) p);
        } else if (p instanceof PredicateRange) {
            rangeList.add((PredicateRange) p);
        }

        // others put in PredicateOther
        else {
            otherList.add((PredicateOther) p);
        }
    }

    /**
     * 是否包含有效的 predicate 条件 == 有些条件不是always true
     */
    public boolean hasEffectivePredicate() {
        if (this == EMPTY_PREDICATE) {
            return false;
        }

        List<Predicate> allPredicate = new ArrayList<>();
        allPredicate.addAll(equalList);
        allPredicate.addAll(rangeList);
        allPredicate.addAll(otherList);

        if (allPredicate.size() == 0) {
            return false;
        }

        Optional<?> any = allPredicate.stream().filter(Predicate::isAlwaysTrue).findAny();
        return any.isPresent();
    }

    // ==================== getter ==========

    public List<EquivalentClass> getEcList() {
        return ecList;
    }

    public List<PredicateEqual> getEqualList() {
        return equalList;
    }

    public List<PredicateRange> getRangeList() {
        return rangeList;
    }

    public List<PredicateOther> getOtherList() {
        return otherList;
    }

    public String getReason() {
        return reason;
    }

    public boolean isSupport() {
        return support;
    }
}
