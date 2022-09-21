package io.trino.sql.rewritemv.predicate;

import io.trino.sql.rewritemv.QualifiedColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PredicateUtil {
    /**
     * range 条件处理:
     * (1) every condition in loose, must also in tight
     * (2) (originalRange - mvRange) can be rewrite
     *
     * @param originalRange 严格条件, = original 的 range 条件
     * @param mvRange 宽松条件, = mv的 range 条件
     * @param ecList 处理需要的ec
     * @param compensation 严格条件 - 宽松条件 得到的补偿条件
     * @return null if all ok, error if loose not cover tight
     */
    public static String rangePredicateCompare(
            List<PredicateRange> originalRange,
            List<PredicateRange> mvRange,
            List<EquivalentClass> ecList,
            List<AtomicWhere> compensation) {

        List<PredicateRange> looseRemain = new ArrayList<>(mvRange);
        List<PredicateRange> tightRemain = new ArrayList<>(originalRange);

        // only support conditions based on ec
        for (EquivalentClass ec : ecList) {

            // find all predicate based on ec
            List<PredicateRange> tightPredicate = originalRange.stream()
                    .filter(pr -> ec.getColumns().contains(pr.getLeft()))
                    .collect(Collectors.toList());

            List<PredicateRange> loosePredicate = mvRange.stream()
                    .filter(pr -> ec.getColumns().contains(pr.getLeft()))
                    .collect(Collectors.toList());

            if (tightPredicate.size() == 0) {
                if (loosePredicate.size() == 0) {
                    continue;
                } else {
                    return "range predicate: cannot cover: " + loosePredicate.get(0);
                }
            }
            looseRemain.removeAll(loosePredicate);
            tightRemain.removeAll(tightPredicate);

            QualifiedColumn column = ec.getColumns().stream().findAny().get();
            PredicateRange shouldSmallRange = PredicateRange.unbound(column, ec);
            for (PredicateRange predicateRange : tightPredicate) {
                shouldSmallRange = shouldSmallRange.intersection(predicateRange);
            }

            PredicateRange shouldLargeRange = PredicateRange.unbound(column, ec);
            for (PredicateRange predicateRange : loosePredicate) {
                shouldLargeRange = shouldLargeRange.intersection(predicateRange);
            }
            if (!shouldLargeRange.coverOtherValue(shouldSmallRange)) {
                return "range predicate: cannot cover: " + shouldLargeRange;
            }
            PredicateRange comp = shouldSmallRange.baseOn(shouldLargeRange);
            if (comp.getEqual() != PredicateRange.PredicateRangeBound.UNBOUND
                    || comp.getLower() != PredicateRange.PredicateRangeBound.UNBOUND
                    || comp.getUpper() != PredicateRange.PredicateRangeBound.UNBOUND) {
                compensation.add(comp);
            }
        }

        if (looseRemain.size() != 0) {
            return "range predicate: condition not in ec (loose):" + looseRemain.get(0).toString();
        }

        if (tightRemain.size() != 0) {
            return "range predicate: condition not in ec (tight)" + tightRemain.get(0).toString();
        }

        return null;
    }


    /**
     * 比较ec 并得到补偿条件
     *
     * @param origEcList = original ec list
     * @param mvEcList = mv ec list
     * @param compensation = 改写时 补偿条件
     * @return null if all ok, error if loose not cover tight
     */
    public static String processPredicateEqual(
            List<EquivalentClass> origEcList,
            List<EquivalentClass> mvEcList,
            List<AtomicWhere> compensation) {

        // make sure all equal in mv, must appear in query
        // 1.1 EquivalentClass contain, eg,
        // mv   eg=[ (colA, colB),       (colC, colD),       (colM, comN)]
        // orig eg=[ (colA, colB, colE), (colC, colD, colY), (colM)      ]
        Map<EquivalentClass, List<EquivalentClass>> map = new HashMap<>(); // ec in original ---- 多个关联ec in mv


        // every non-trivial ec in mv, must in original
        for (EquivalentClass mvEc : mvEcList) {
            if (mvEc.getColumns().size() < 2) {
                continue;
            }

            QualifiedColumn oneColumnInEc = mvEc.getColumns().stream().findAny().get();

            Optional<EquivalentClass> origEcOpt = origEcList.stream().filter(x -> x.contain(oneColumnInEc)).findAny();
            if (origEcOpt.isEmpty()) {
                return ("where: mv has equal predicate(s) not exists in query - 1");
            }
            EquivalentClass origEc = origEcOpt.get();

            if (!origEc.getColumns().containsAll(mvEc.getColumns())) {
                // eg (colM, colN) --- (colM)
                return ("where: mv has equal predicate(s) not exists in query - 2");
            }

            if (map.get(origEc) == null) {
                List<EquivalentClass> list = new ArrayList<>();
                list.add(mvEc);
                map.put(origEc, list);
            } else {
                map.get(origEc).add(mvEc);
            }
        }

        // 处理剩下的 ec条件
        List<EquivalentClass> tmp1 = new ArrayList<>(origEcList.size());
        for (EquivalentClass origEc : origEcList) {
            List<EquivalentClass> toRemove = map.get(origEc);
            if (toRemove == null || toRemove.size() == 0) {
                tmp1.add(origEc);
            } else {
                // 从 origEc中 删除 toRemove中保留的关系
                EquivalentClass remain = new EquivalentClass();
                Set<QualifiedColumn> big = new HashSet<>();
                for (EquivalentClass ecToRemove : toRemove) {
                    QualifiedColumn each = ecToRemove.getColumns().stream().findAny().get();
                    remain.add(each);
                    big.addAll(ecToRemove.getColumns());
                }

                for (QualifiedColumn c : origEc.getColumns()) {
                    if (!big.contains(c)) {
                        remain.add(c);
                    }
                }
                tmp1.add(remain);
            }
        }

        for (EquivalentClass ec : tmp1) {
            if (ec.getColumns().size() >= 2) {
                // 只有一个元素的 EquivalentClassBase是没有 = 关系的 {colA}
                Set<QualifiedColumn> columns = ec.getColumns();
                List<QualifiedColumn> list = new ArrayList<>(columns);
                QualifiedColumn col0 = list.get(0);
                for (int i = 1; i < list.size(); i++) {
                    compensation.add(new PredicateEqual(null, col0, list.get(i)));
                }
            }
        }

        return null;
    }

}
